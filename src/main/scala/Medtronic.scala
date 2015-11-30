/*
 * Spark streaming code for updating max, min, avg on user data
 */

// scalastyle:off println
package org.apache.spark.test.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream._
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import com.github.nscala_time.time.Imports._
import java.util.Date
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder
import org.apache.commons.io.FileUtils
import java.nio.file.Files
import org.apache.lucene.util.Version

/**
 * Parses file sent across the wire
 * ES run w/ export  JAVA_OPTS="-Xms256m -Xmx512m -XX:-UseSuperWord"
 */

object PatientData {
  

   case class TrialRecord(variance: Int, percentage: Float, trialId: String, patientId: String, visitName: String, value: Int, device: String, startDate: Date, startTime: Int, endDate: Date, endTime: Int)        
   
   case class SummaryRecord(trialId: String, private var _patientId: String, private var _visitName:String, private var _max:Int, private var _min:Int, private var _mean:Int)
   {
        // trialId: String, patientId: String, visitName:String, max:Int, min:Int, mean:Int)
     //private var _max = 0
     def max = _max 
     def max_= (value:Int):Unit = _max = value  
     def min = _min 
     def min_= (value:Int):Unit = _min = value  
     def mean = _mean 
     def mean_= (value:Int):Unit = _mean = value 
     def visitName = _visitName 
     def patientId = _patientId 
     
     def this()
     {
       this("","","",0,0,0)
     }
     
   }
   
     /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      //logInfo("Setting log level to [WARN] for streaming example." +
      //   " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.ERROR)

    }
  }
  
  

   
	/**
	 * Method to group summary records
	 * @param it
	 * @param optionSummary
	 * @return
	 */
	def groupSummaryRecords(it:Iterator[SummaryRecord],  optionSummary:Option[SummaryRecord]):Option[SummaryRecord] = {
	    var count=0;
		var min=Integer.MAX_VALUE;
		var max=0;
		var total=0;
	    var summaryRecord1= new SummaryRecord()
		var i=0
		while(it.hasNext) {
			var summaryRecord:SummaryRecord = it.next();
			println("sr: " + summaryRecord.toString)
			if(i==0) {
				summaryRecord1 = summaryRecord;
			}
			if(summaryRecord.mean !=null.asInstanceOf[Int   ]){
				total = total+ summaryRecord.mean;
				count+= 1;
			}
			if(summaryRecord.max!=null.asInstanceOf[Int   ] && summaryRecord.max>max) {
				max = summaryRecord.max;
			}
			if(summaryRecord.min!=null.asInstanceOf[Int   ] && summaryRecord.min<min) {
				min = summaryRecord.min;
			}
			i+= 1;
		}
		if(summaryRecord1==null && optionSummary.isDefined) {
          println("returning some sr !!!!!!")
          return Some(summaryRecord1)
		}
		if(summaryRecord1==null) {
		  println("null")
			return None
			 //null;
		}
		summaryRecord1.max =max;
		summaryRecord1.min =min;
	    var mean = 0;
		if(count!=0){
			mean = total/count;
			println("mean: " + mean)
		}
		
		//If optionSummary not null than update the max min and average as per the optionSummary
		if(optionSummary.nonEmpty) {
            println("update max min")
			 Logger.getRootLogger.info("Found the old state for summary patientId:"+optionSummary.get.patientId+" visitName:"+optionSummary.get.visitName)
			if(optionSummary.get.max!=null.asInstanceOf[Int   ] && optionSummary.get.max>max) {
				summaryRecord1.max =optionSummary.get.max;
				println("max: " + summaryRecord1.max)
			}
			if(optionSummary.get.min!=null.asInstanceOf[Int   ] && optionSummary.get.min<min) {
				summaryRecord1.min=optionSummary.get.min;
			}
			if (optionSummary.get.mean!=null.asInstanceOf[Int   ]  ) {
				mean=(optionSummary.get.mean+mean)/2;
			}
		}
		
		
		summaryRecord1.mean = mean
	    return Some(summaryRecord1)
		
	}
	    
  def summary(trialRecordDStream: DStream[(String, TrialRecord)]):DStream[(String,SummaryRecord)] =
  {
    val summaryRecordDStream: DStream[(String, SummaryRecord)] = trialRecordDStream.map( trialRecordTuple =>
    {
      //Generate the summary records pair from study trial records dstream
      val trialRecord = trialRecordTuple._2;   
      val summaryRecord = new SummaryRecord(trialRecord.trialId, trialRecord.patientId, trialRecord.visitName, trialRecord.value, trialRecord.value, trialRecord.value);
      val key:String = trialRecord.trialId + "_"+ trialRecord.patientId + "_" + trialRecord.visitName + "_" + trialRecord.device 
      println("Summary Key: " + key)
	  (key, summaryRecord);
		
    }
      )
    //Group the summary dstream by key for aggregation
    val summaryGroupedDStream: DStream[(String,Iterable[Medtronic.SummaryRecord])] = summaryRecordDStream.groupByKey();
    println("grouped key !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + summaryGroupedDStream.toString())
    
    //Map the grouped dstream to generate summary records min max and avg
	 val summaryRecordDStream2:DStream[(String, Option[SummaryRecord])]  = summaryGroupedDStream.map( t => 
	  {
		val summaryRecord1 = groupSummaryRecords(t._2.iterator, None: Option[SummaryRecord]);
		println("sr1: ************" + summaryRecord1.toString())
		(t._1, summaryRecord1);
			}
		);
	
    
    //Calling updateStateByKey to maintain the state of the summary object  - only from the stream
    val summaryRecordUpdatedDStream = summaryRecordDStream.updateStateByKey(

        (v1:Seq[SummaryRecord], v2:Option[SummaryRecord]) =>
        {
		  val summaryRecord1 = groupSummaryRecords(v1.iterator, v2)
		  summaryRecord1
        }

        );

	summaryRecordUpdatedDStream
  }
  
  def main(args: Array[String]) {
  
    
   setStreamingLogLevels()

   // Parameters
    // Create the context with a 1 second batch size
    val batchSize = 10;
    val filter1 = "HR DAILY"
    val filter2 = "MEASUREMENT"
    val filter3 = "SUMMARY (MEAN) HEART RATE"
    val HRbaseValue = 50
    //val alertThreashold = 10
    val format = new java.text.SimpleDateFormat("ddMMMyyyy")
    
    val sparkConf = new SparkConf().setAppName("Medtronic2")
    sparkConf.set("es.index.auto.create", "true")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchSize))

    ssc.checkpoint("./checkpoint")
  
    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    //val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val lines = ssc.textFileStream("file:///Users/mlieber/projects/spark/test/data/")
    var max, min, avg =0;
    val filteredLines = lines.filter(line => line.contains(filter1)&&line.contains(filter2)&&line.contains(filter3))
    
    println("Trial record processing" )
    val trialRecordDStream  =
          filteredLines.map ( lin =>
          {  
            val columns = lin.split('|')
            val variance = (columns(12).toInt - HRbaseValue )
            val percentage:Float = (variance*100)/HRbaseValue 
            println("Percentage here: " + percentage)
            // Example data CV205-005|000100074|05FEB1945|M|S01|13JUL2015|145433|21JUL2015|030935|MEASUREMENT|HR DAILY|SUMMARY (MEAN) HEART RATE|59||BEATS/MIN|1|N11150612006CDA|||||||17JUL2015|000000|18JUL2015|000000
            // variance/percentage/trialId/patientId/visitName/result/device/startDate/startTime/endDate/endTime
            val varianceRecord = TrialRecord(variance, percentage, columns(0), columns(1), columns(4), columns(12).toInt, columns(16), 
        	                format.parse(columns(23)), columns(24).toInt , format.parse(columns(25)), columns(26).toInt )
            // return tuple KV
            (columns(0) + '_' + columns(1) + '_' + columns(4)+'_'+columns(16),
        	  varianceRecord)
          }
          )
        
     val summaryUpdatedDStream:DStream[(String,SummaryRecord)]  = summary(trialRecordDStream)
 
     
     summaryUpdatedDStream.foreachRDD(lineRDD =>   
     {
     print("summary value " + lineRDD)
      EsSpark.saveToEsWithMeta(lineRDD, "indexmatt/summarydata") 
   }
   )
   
   
    
     
    // just counting lines
    /*
    val wordPairs2 = pairRDD.map(x => ("number of lines2: ", 1))
    val wordCount2 = wordPairs2.reduceByKey(_ + _)
    wordCount2.collect().foreach(a => print(a) )
    */
   trialRecordDStream.foreachRDD(lineRDD =>   
   {
     print("value " + lineRDD)
      EsSpark.saveToEsWithMeta(lineRDD, "indexmatt/trialdata") 
   }
   )
 
     
  //  val documents = sc.esRDD("medtronics/alerts", "?q=variable:62")


    
    // Start
    ssc.start()    
    ssc.awaitTermination()
    

    }
  }
