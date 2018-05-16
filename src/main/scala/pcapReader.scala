import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType}


/**
 * This doesn't actually read pcaps.  It reads the csv files produced by tshark in Vu's script.
 */
object pcapReader {

  
  /**
   * Fields in the file are:
   * 1. frame.number 
   * 2. frame.time_epoch 
   * 3. ip.src 
   * 4. tcp.srcport 
   * 5. ip.dst 
   * 6. tcp.dstport 
   * 7. ip.proto 
   * 8. frame.len 
   * 9. tcp.seq 
   * 10. tcp.ack 
   * 11. tcp.options.mptcp.rawdataseqno (dsn)
   * 12. tcp.options.mptcp.rawdataack (dack)
   * 
   * 
   */
	val pcapSchema = StructType(Array(
			StructField("framenumber", LongType, false),
			StructField("timestamp", DoubleType, true),
			StructField("src", StringType, false),
			StructField("srcport", IntegerType, false),
			StructField("dst", StringType, true),
			StructField("dstport", IntegerType, false),
			StructField("proto", IntegerType, true),
			StructField("framelen", IntegerType, true),
			StructField("tcpseq", IntegerType, true),
			StructField("tcpack", IntegerType, true),
			StructField("dsn", LongType, true),  // because there is no unsigned 32 bit type afaik
			StructField("dack", LongType, true)));

	/**
	 * read in the task events file(s)
	 */
	def readPcap(spark: SparkSession, filename: String): Dataset[Row] = {
			return spark.read
					.option("sep","\t")
				  .option("nullValue","NULL")
				  .option("mode","DROPMALFORMED")
				  .schema(pcapSchema)
				  .csv(filename)
	}

	/**
	 * Schema and loading code for DITG receiver logs.
	 * These are produced by running something like:
	 * ~/ditg/bin/ITGDec receiver.log -o receiverlog.out
	 */
	val ditgSchema = StructType(Array(
	    StructField("framenumber", LongType, false),
	    StructField("tx_hour", IntegerType, false),
	    StructField("tx_minute", IntegerType, false),
	    StructField("tx_second", DoubleType, false),
	    StructField("rx_hour", IntegerType, false),
	    StructField("rx_minute", IntegerType, false),
	    StructField("rx_second", DoubleType, false),
	    StructField("datasize", IntegerType, false)));

	/**
	 * read in the DITG receiver file
	 */
	def readDitg(spark: SparkSession, filename: String): Dataset[Row] = {
	  return spark.read
					.option("sep"," ")
				  .option("nullValue","NULL")
				  .option("mode","DROPMALFORMED")
				  .option("ignoreLeadingWhiteSpace", true)
				  .option("ignoreTrailingWhiteSpace", true)				  
				  .schema(ditgSchema)
				  .csv(filename);
	}
	
	
}


