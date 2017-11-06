import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._ // for `when`
import org.apache.spark.sql.expressions.Window
import org.apache.spark.api.java.StorageLevels._
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.rdd.RDD

/**
 * This will be code to turn job/task event data into a table of
 * job/task data with arrive/start/end times and total durations computed
 */

object PcapProcessor {
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._


  /**
   * Load the src and dst pcap files and pick out the first time where each
   * MPTCP packet that is:
   * 1. sent
   * 2. recieved
   * 
   * This lets us compute the latency.
   * 
   * This function ignores packets that are never recieved (that have latency
   * of infinity).  In principle tcp should re-send packets until they get through,
   * so any missing receptions are because of an error or truncation in the log.
   * 
   * The loadProcessPcapFull() function below does all this and more, but
   * this should be faster because it takes a coarser grouping of the frames
   * and doesn't do as much processing.
   * 
   * TODO: the DSN will eventually roll over.  There should be a time difference limit
   *       in the join.
   */
  def loadProcessPcapLatency(srcfile:String, dstfile:String): Dataset[Row] = {
    
    val first_w = Window.partitionBy($"dsn").orderBy($"timestamp".asc)
    
    val src_data = pcapReader.readPcap(spark, srcfile)
        .filter("proto=6").filter("dsn is not null").filter("dack is not null")
        .groupBy($"dsn").agg(min($"timestamp").alias("src_timestamp"))
    val dst_data = pcapReader.readPcap(spark, dstfile)
        .filter("proto=6").filter("dsn is not null").filter("dack is not null")
        .groupBy($"dsn").agg(min($"timestamp").alias("dst_timestamp"))
    
    return src_data.as("src").join(dst_data.as("dst"), "dsn")
      .withColumn("latency", $"dst_timestamp" - $"src_timestamp")
  }
  
  
  /**
   * Compute the CDF and CCDF of the latency data.
   * 
   * I tried doing this nicely in Spark, but ended up just collecting the
   * data and computing it in the driver.  For the number of data points
   * we have, this should be fine.
   */
  def latencyCdf(ldf:Dataset[Row], numpoints:Integer): (Array[Double], Array[Double]) = {
    val ldfcount:Long = ldf.count();
    val ldfcount_d = ldfcount.toDouble
    val n = if (numpoints > 0) numpoints+1 else ldfcount+1
    val incr = if (numpoints > 0) math.ceil(ldfcount_d/numpoints).toInt else 1;
    val cdfx = new Array[Double] (n.toInt);
    val cdfy = new Array[Double] (n.toInt);
    val ldfs = ldf.select("latency").orderBy("latency").collect.map(x => x.getDouble(0));
    
    var i = 0;
    var bin = 0;
    println(incr)
    ldfs.foreach( x => {
        //println(""+i+"\t"+bin)
        if ((i % incr) == 0) {
            cdfx(bin) = x;
            cdfy(bin) = i.toDouble/ldfcount_d;
            bin += 1;
        }
        i += 1;
    })
    
    return (cdfx, cdfy)
  }
  
    
  
  /**
   * Estimate the pdf of latency data using mllib's KernelDensity class.
   */
  def latencyPdf(data:Dataset[Row]): (Array[Double], Array[Double]) = {

		  val rtmp = data.select(min("latency"),max("latency")).first;
		  val (lmin, lmax) = (rtmp.getDouble(0), rtmp.getDouble(1));
		  val numpoints = 250d;
		  val evalpoints = lmin to lmax by ((lmax-lmin)/numpoints) toArray;
		  val bandwidth = ((lmax-lmin)/numpoints);

		  val kd = new KernelDensity()
				  .setSample(data.select("latency").map( x => x.getDouble(0) ).rdd)
				  .setBandwidth(bandwidth);

		  // Find density estimates for the given values
		  val densities = kd.estimate(evalpoints);

		  //return evalpoints.zip(densities)
		  return (evalpoints, densities)
  }
  
  
  /**
   * Compute the ccdf of latency data from the PDF estimated by latencyPdf().
   * 
   */
  def latencyCcdf(pdf_cols: (Array[Double], Array[Double]), title: String): Array[Map[String,Any]] = {
    if (pdf_cols._1.length != pdf_cols._2.length) {
      println("ERROR: latencyCcdf() data arrays must be the same length.")
      return Array()
    }
    
    val pdf = pdf_cols._1.zip(pdf_cols._2)
    var cum:Double = 1.0
    var last_x = -1.0
    
    // the map should operate sequentially
    val ccdf = pdf.map( x => {
      if (last_x >= 0) {
    	  cum -= (x._1 - last_x) * x._2
      }
      last_x = x._1
      (x._1, cum)
    })
        
    return ccdf.map( x => Map("x"->x._1, "y"->x._2, "title"->title) )
  }
  
  
  /**
   * Compute a histogram of latency data.
   */
  def latencyHistogram(jpcap_l: Dataset[Row], bins:Int = 50): (Array[Double], Array[Long]) = {
    return jpcap_l.select("latency").map(x => x.getDouble(0)).rdd.histogram(bins)
  }
  
  
  /**
   * Loads the corresponding src/dst pcaps
   * - group the src frames by dsn and assign "job" numbers
   * - within each job on the src side, assign "task" numbers to the frames (re-sends of the frame)
   * - sort by job/task numbers, and assign a global task index to each frame.
   *   This can be used to plot the experiment path
   * - Join the job/task index and src pcap to the dst pcap.
   *   Not every frame is received at the dst, so this is a left outer join
   * - This also produces jsrc_pcap and jdst_pcap, but I should get rid of those.
   * 
   * The three Datasets returned are persisted.
   * 
   * TODO: the DSN will eventually roll over.  There should be a time difference limit
   *       in the join.
   * 
   */
  def loadProcessPcapFull(srcfile:String, dstfile:String): (Dataset[Row],Dataset[Row],Dataset[Row]) = {

    val dsn_partition_w = Window.partitionBy($"dst", $"dsn").orderBy($"timestamp".asc)
    val sorted_w = Window.orderBy($"timestamp".asc)
    val jt_sorted_w = Window.orderBy($"jobid".asc, $"taskid".asc)
    val taskid_w = Window.partitionBy($"jobid").orderBy($"timestamp".asc)

    // assume all packets are seen first at the src
    val src_pcap = pcapReader.readPcap(spark, srcfile)
        .filter("proto=6").filter("dsn is not null").filter("dack is not null")
    val dst_pcap = pcapReader.readPcap(spark, dstfile)
        .filter("proto=6").filter("dsn is not null").filter("dack is not null").persist(MEMORY_AND_DISK_SER);
    println("src records: "+src_pcap.count())
    println("dst records: "+dst_pcap.count())
    
    // unfortunately there's no way to assign an ordering and dense rank to the partitions.
    // otherwise we could compute the job and task IDs at the same time.
    // dense_rank doesn't do it because we want to partition by one thing, but order by timestamp.
    // This will compute job IDs by grouping the src packets by (dst,dstport,dsn,dack),
    // taking the first row from each partition, and then assigning a rank to those representatives.
    val jobid_index = src_pcap.withColumn("rn", row_number.over(dsn_partition_w)).where("rn=1").drop("rn")
        .withColumn("jobid", row_number.over(sorted_w))
        .drop("timestamp", "framenumber", "proto", "tcpseq", "tcpack", "framelen", "src", "srcport", "dstport", "dack")
    
    // now we have to join this back onto the source_pcap
    // and on the last line, compute the task IDs
    val jsrc_pcap = src_pcap.as("src").join(jobid_index.as("job"), $"src.dst"===$"job.dst" && $"src.dsn"===$"job.dsn", "left_outer")  // && $"src.dack"===$"job.dack", "left_outer")
        .withColumn("taskid", row_number.over(taskid_w))
        .withColumn("tasknum", row_number.over(jt_sorted_w))
        .drop($"job.dst").drop($"job.dsn")
        .orderBy("tasknum")
        .persist(MEMORY_AND_DISK_SER);
    
    // now we finally have a table of jobid, taskid and associated fields
    val taskid_index = jsrc_pcap.select("jobid", "taskid", "tasknum", "dst", "dstport", "dsn", "dack", "tcpseq", "tcpack").persist(MEMORY_AND_DISK_SER);
    
    // join the job/task IDs onto the packets at the dst
    val jdst_pcap = dst_pcap.as("dst").join(taskid_index.as("p"), $"dst.dst"===$"p.dst" &&$"dst.dstport"===$"p.dstport" && $"dst.dsn"===$"p.dsn" && $"dst.dack"===$"p.dack" && $"dst.tcpseq"===$"p.tcpseq" && $"dst.tcpack"===$"p.tcpack", "left_outer")
        .filter($"tasknum".isNotNull)
        .drop($"p.dst").drop($"p.dstport").drop($"p.dsn").drop($"p.dack").drop($"p.tcpseq").drop($"p.tcpack")
        .orderBy("tasknum")
        .persist(MEMORY_AND_DISK_SER);
    
    //jsrc_pcap.show()
    //jdst_pcap.show()
    
    // finally, join the src and dst pcaps into one big dataset.
    // this can be used to see the packets dropped (dst.timestamp will be null), or latency.
    val jpcap = jsrc_pcap.as("src")
        .join(dst_pcap.as("dst"), $"dst.dst"===$"src.dst" && $"dst.dstport"===$"src.dstport" && $"dst.dsn"===$"src.dsn" && $"dst.dack"===$"src.dack" && $"dst.tcpseq"===$"src.tcpseq" && $"dst.tcpack"===$"src.tcpack", "left_outer")
        .drop($"dst.src").drop($"dst.srcport").drop($"dst.dst").drop($"dst.dstport").drop($"dst.proto").drop($"dst.framelen")
        .drop($"dst.tcpseq").drop($"dst.tcpack").drop($"dst.dsn").drop($"dst.dack").drop($"dst.framenumber").drop($"src.framenumber")
        .orderBy("tasknum")
        .persist(MEMORY_AND_DISK_SER);
    
    dst_pcap.unpersist();
    taskid_index.unpersist();
    
    return (jsrc_pcap, jdst_pcap, jpcap)

  }
  
  
  /**
   * Extract the experiment path from jobid=start to jobid=end and return the 
   * data in a form suitable for plotting with Vegas-viz.
   * 
   * The experiment path is a sequence of frames sorted by tasknum (that is, by
   * jobid and taskid), with the timestamp recorded at both the src and dst ends.
   * The dst timestamp may be null if the frame was dropped, in which case the
   * dst frame will not appear in the returned array.
   * 
   * The job and task IDs are not currently included in the result, because we
   * aren't using them in the plots.
   * 
   * Example return value:
   * Array(Map(ip -> "dst: 10.1.2.2 -> 10.1.1.2", pktnum -> 5129, t -> 1.5088584996016316E9), 
   *       Map(ip -> "src: 10.1.2.2 -> 10.1.1.2", pktnum -> 5129, t -> 1.5088584995073369E9), 
   *       Map(ip -> "dst: 10.1.3.2 -> 10.1.1.2", pktnum -> 5130, t -> 1.5088584995606186E9), 
   *       Map(ip -> "src: 10.1.3.2 -> 10.1.1.2", pktnum -> 5130, t -> 1.5088584995078845E9), 
   *       Map(ip -> "dst: 10.1.3.2 -> 10.1.1.2", pktnum -> 5131, t -> 1.5088584995626194E9))
   */
  def experimentPaths(jpcap:Dataset[Row], start:Int, end:Int): Array[Map[String,Any]] = {
    
    val tmp = jpcap.filter($"jobid" >= start && $"jobid" <= end).persist();

    val result =  tmp.filter($"dst.timestamp".isNotNull).select(concat_ws(" ", lit("dst:"), $"src", lit("->"), $"dst"), $"tasknum", $"dst.timestamp".as("timestamp"))
            .union(tmp.select(concat_ws(" ", lit("src:"), $"src", lit("->"), $"dst"), $"tasknum", $"src.timestamp".as("timestamp"))).orderBy("tasknum")
            .collect().map( x => Map("ip"->x.getString(0), "pktnum"->x.getInt(1), "t"->x.getDouble(2)))

    tmp.unpersist()

    if (result.length == 0) {
        println("ERROR: experimentPaths resulted in empty dataset")
    }

    return result
  }

  
  
  

  
}


