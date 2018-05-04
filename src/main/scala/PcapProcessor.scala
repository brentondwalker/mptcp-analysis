import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._ // for `when`
import org.apache.spark.sql.expressions.Window
import org.apache.spark.api.java.StorageLevels._
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util.DateTimeUtils


/**
 * This will be code to turn job/task event data into a table of
 * job/task data with arrive/start/end times and total durations computed
 */

object PcapProcessor {
  val lspark = SparkSession.builder.getOrCreate()
  import lspark.implicits._


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
   * There have been problems of negative latencies.  Two possible causes of that are:
   * 
   * - we captured some reverse traffic (like ACK).  Then the dst is the sender,
   *   and when we compute dst.timestamp - src.timestamp we get the negative of its
   *   actual latency.  We now include the packets' dst in the table, so those cases
   *   can be filtered out.
   *   
   * - the packet has multiple sends, and multiple receives, but we only capture the
   *   last send.  In this case, if we just group by DSN, we can see packets that are
   *   received before any recorded transmissions.  It seems to happen only at the start
   *   of a capture, especially for the redundant scheduler.  The most correct solution
   *   here is to make sure that any received frame actually has a recorded
   *   transmission, *or* be more careful in starting the experiments.  The simple,
   *   and close to correct, solution is to just filter out negative latencies.
   * 
   * TODO: the DSN will eventually roll over.  There should be a time difference limit
   *       in the join.
   *       
   *  TODO: how to filter out traffic from dst back to src?
   */
  def loadProcessPcapLatency(srcfile:String, dstfile:String): Dataset[Row] = {
    
    val src_data = pcapReader.readPcap(lspark, srcfile)
        .filter("proto=6").filter("dsn is not null").filter("dack is not null")
        .groupBy($"dsn").agg(min($"timestamp").alias("src_timestamp"),min("dst").alias("dst"),min("framelen").alias("src_len"))
    val dst_data = pcapReader.readPcap(lspark, dstfile)
        .filter("proto=6").filter("dsn is not null").filter("dack is not null")
        .groupBy($"dsn").agg(min($"timestamp").alias("dst_timestamp"),min("framelen").alias("dst_len"))
    
    return src_data.as("src").join(dst_data.as("dst"), "dsn")
      .withColumn("latency", $"dst_timestamp" - $"src_timestamp")
  }
  
  
  /**
   * If we are processing normal TCP (not TCPMP) then there are no DSN or DACK.
   * 
   * The resulting latency table is the same, though.
   */
  def loadProcessPcapLatencyTcp(srcfile:String, dstfile:String): Dataset[Row] = {
    
    val src_data = pcapReader.readPcap(lspark, srcfile)
        .filter("proto=6").filter("tcpseq is not null").filter("tcpack is not null")
        .groupBy($"tcpseq").agg(min($"timestamp").alias("src_timestamp"),min("dst").alias("dst"),min("framelen").alias("src_len"))
    val dst_data = pcapReader.readPcap(lspark, dstfile)
        .filter("proto=6").filter("tcpseq is not null").filter("tcpack is not null")
        .groupBy($"tcpseq").agg(min($"timestamp").alias("dst_timestamp"),min("framelen").alias("dst_len"))
    
    return src_data.as("src").join(dst_data.as("dst"), "tcpseq")
      .withColumn("latency", $"dst_timestamp" - $"src_timestamp")
  }

  
  
  /**
   * This is a more involved way to load latency data, where we check that each
   * received frame has a corresponding sent frame.  This eliminates a mismatch
   * that can happen sometimes when the packets are flying already when tshark starts.
   * It is a rare problem (maybe 30 packets out of 100,000), so it's really better
   * to just filter the negative latencies out, or be more careful when starting
   * the experiment.
   * 
   * TODO: this is not working correctly on wget trace #8.  The tcp seq number just don't match.
   */
  def loadProcessPcapLatencyCareful(srcfile:String, dstfile:String): Dataset[Row] = {
    val dsn_partition_w = Window.partitionBy($"dst", $"dsn").orderBy($"timestamp".asc)
    
    val src_data = pcapReader.readPcap(lspark, srcfile)
        .filter("proto=6").filter("dsn is not null").persist(MEMORY_AND_DISK_SER);

    val src_timestamp_data = src_data.groupBy($"dsn").agg(min($"timestamp").alias("src_timestamp"),min("dst").alias("dst"),min("framelen").alias("src_len"));
    
    val dst_data = pcapReader.readPcap(lspark, dstfile)
        .filter("proto=6").filter("dsn is not null")
        .withColumn("rxnum", row_number.over(dsn_partition_w)).where("rxnum=1");

    // this is where we make sure that each of our "first" frames collected at the dst
    // has a corresponding frame collected at the src.
    val dst_timestamp_data = dst_data.as("dst1").join(src_data.as("src1"), 
          $"src1.dsn"===$"dst1.dsn" 
          && $"src1.src"===$"dst1.src"
          && $"src1.srcport"===$"dst1.srcport"
          && $"src1.dst"===$"dst1.dst"
          && $"src1.dstport"===$"dst1.dstport"
          && $"src1.tcpseq"===$"dst1.tcpseq"
          && $"src1.tcpack"===$"dst1.tcpack")
        .select($"dst1.dsn".alias("dsn"), $"dst1.timestamp".alias("dst_timestamp"), $"dst1.framelen".alias("dst_len"))
        
    return src_timestamp_data.as("src").join(dst_timestamp_data.as("dst"), "dsn")
      .withColumn("latency", $"dst_timestamp" - $"src_timestamp")
  }
  
  
  /**
   * Use this to track down issues with negative latencies.
   * Hopefully they are all solved anyway.
   */
  def checkNegativeLatencies(pcap_l_list: Array[Dataset[Row]]) {
      for (pcap_l <- pcap_l_list) {
        println("total records: "+pcap_l.count)
        println("negative latencies:"+pcap_l.filter($"latency" < 0.0).count)
        pcap_l.filter($"latency" < 0.0).show
      }
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
    val cdfx = new ListBuffer[Double] ();
    val cdfy = new ListBuffer[Double] ();
    val ldfs = ldf.select("latency").orderBy("latency").collect.map(x => x.getDouble(0));
    val rtmp = ldf.select(min("latency"),max("latency")).first;
    val (lmin, lmax) = (rtmp.getDouble(0), rtmp.getDouble(1));
    val x_incr = (lmax - lmin)/n
    val y_incr = ldfcount/n
    
    var i = 0;
    var bin = 0;
    var next_x_threshold = lmin
    var next_y_threshold = 0l
    //println(x_incr)
    //println(y_incr)
    ldfs.foreach( x => {
        //println(""+i+"\t"+bin)
        if (x >= next_x_threshold || i >= next_y_threshold) {
            cdfx += x;
            cdfy += i.toDouble/ldfcount_d;
            bin += 1;
            next_x_threshold += x_incr
            next_y_threshold += y_incr
        }
        i += 1;
    })
    
    return (cdfx.toArray, cdfy.toArray)
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
   * Compute the CCDF of the latency data.
   * 
   * I tried doing this nicely in Spark, but ended up just collecting the
   * data and computing it in the driver.  For the number of data points
   * we have, this should be fine.
   * 
   * TODO: The size of the array this returns is problematic for large datasets.
   *       We need to gradually scale the increment down.
   */
  def latencyCcdf(ldf:Dataset[Row], numpoints:Integer): (Array[Double], Array[Double]) = {
    val ldfcount:Long = ldf.count();
    val ldfcount_d = ldfcount.toDouble
    val n = if (numpoints > 0) numpoints+1 else ldfcount+1
    val cdfx = new ListBuffer[Double] ();
    val cdfy = new ListBuffer[Double] ();
    val ldfs = ldf.select("latency").orderBy("latency").collect.map(x => x.getDouble(0));
    val rtmp = ldf.select(min("latency"),max("latency")).first;
    val (lmin, lmax) = (rtmp.getDouble(0), rtmp.getDouble(1));
    val x_incr = (lmax - lmin)/n
    val y_incr = ldfcount/n
    
    var i = 0;
    var bin = 0;
    var next_x_threshold = lmin
    var next_y_threshold = 0l
    val tail_y_threshold = ldfcount * 997 / 1000
    //println(x_incr)
    //println(y_incr)
    //println(tail_y_threshold)
    ldfs.foreach( x => {
        //println(""+i+"\t"+bin)
        if (x >= next_x_threshold || i >= next_y_threshold || i >= tail_y_threshold) {
            cdfx += x;
            cdfy += 1.0 - i.toDouble/ldfcount_d;
            bin += 1;
            next_x_threshold += x_incr
            next_y_threshold += y_incr
        }
        i += 1;
    })
    
    return (cdfx.toArray, cdfy.toArray)
  }

  
  
  /**
   * Compute the ccdf of latency data from the PDF estimated by latencyPdf().
   * 
   */
  def latencyKernelCcdf(pdf_cols: (Array[Double], Array[Double]), title: String): Array[Map[String,Any]] = {
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
   * - group the src frames by DSN and assign "job" numbers in the order the DSNs first appear
   * - within each job on the src side, assign "task" numbers to the frames (re-sends of the frame)
   * - sort by job/task numbers, and assign a global task index to each frame.
   *   This can be used to plot the experiment path
   * - Join the job/task index and src pcap to the dst pcap.
   *   Not every frame is received at the dst, so this is a left outer join
   * 
   * TODO: the DSN will eventually roll over.  There should be a time difference limit
   *       in the join.
   *       
   * XXX: the caller can filter the result by dst in order to remove acks and other traffic
   *      in the other direction, but then the jobIDs are not a dense index anymore.
   * 
   */
  def loadProcessPcapFull(srcfile:String, dstfile:String): Dataset[Row] = {

    val dsn_partition_w = Window.partitionBy($"dst", $"dsn").orderBy($"timestamp".asc)
    val sorted_w = Window.orderBy($"timestamp".asc)
    val jt_sorted_w = Window.orderBy($"jobid".asc, $"taskid".asc)
    val taskid_w = Window.partitionBy($"jobid").orderBy($"timestamp".asc)

    // assume all packets are seen first at the src
    val src_pcap = pcapReader.readPcap(lspark, srcfile)
        .filter("proto=6").filter("dsn is not null")  //.filter("dack is not null")
    val dst_pcap = pcapReader.readPcap(lspark, dstfile)
        .filter("proto=6").filter("dsn is not null")   //.filter("dack is not null").persist(MEMORY_AND_DISK_SER);
    
    // unfortunately there's no way to assign an ordering and dense rank to the partitions.
    // otherwise we could compute the job and task IDs at the same time.
    // dense_rank doesn't do it because we want to partition by DSN, but order the partitions by timestamp.
    // This will compute job IDs by grouping the src packets by (dst,dstport,dsn,dack),
    // taking the first row from each partition, and then assigning a rank to those representatives.
    val jobid_index = src_pcap.withColumn("rn", row_number.over(dsn_partition_w)).where("rn=1").drop("rn")
        .withColumn("jobid", row_number.over(sorted_w))
        .drop("timestamp", "framenumber", "proto", "tcpseq", "tcpack", "framelen", "src", "srcport", "dstport", "dack")
    
    // now we have to join the job IDs back onto the source_pcap
    // and compute the task IDs
    val jsrc_pcap = src_pcap.as("src").join(jobid_index.as("job"), $"src.dst"===$"job.dst" && $"src.dsn"===$"job.dsn", "left_outer")  // && $"src.dack"===$"job.dack", "left_outer")
        .withColumn("taskid", row_number.over(taskid_w))
        .withColumn("tasknum", row_number.over(jt_sorted_w))
        .drop($"job.dst").drop($"job.dsn")
        .orderBy("tasknum")
    
    // now we finally have a table of jobid, taskid and associated fields
    //val taskid_index = jsrc_pcap.select("jobid", "taskid", "tasknum", "dst", "dstport", "dsn", "dack", "tcpseq", "tcpack")
    
    // finally, join the src and dst pcaps into one big dataset.
    // this can be used to see the packets dropped (dst.timestamp will be null), or latency.
    val jpcap = jsrc_pcap.as("src")
        .join(dst_pcap.as("dst"), $"dst.dst"===$"src.dst" && $"dst.dstport"===$"src.dstport" && $"dst.dsn"===$"src.dsn" && $"dst.dack"===$"src.dack" && $"dst.tcpseq"===$"src.tcpseq" && $"dst.tcpack"===$"src.tcpack", "left_outer")
        .drop($"dst.src").drop($"dst.srcport").drop($"dst.dst").drop($"dst.dstport").drop($"dst.proto").drop($"dst.framelen")
        .drop($"dst.tcpseq").drop($"dst.tcpack").drop($"dst.dsn").drop($"dst.dack").drop($"dst.framenumber").drop($"src.framenumber")
        .orderBy("tasknum")
    
    return jpcap
  }
  
  
  /**
   * If we are processing normal TCP (not TCPMP) then there are no DSN or DACK.
   * 
   * The resulting jpcap may be a little different too.  It may lack dsn and dack columns,
   * they may have all null entries.
   */
  def loadProcessPcapFullTcp(srcfile:String, dstfile:String): Dataset[Row] = {

    val tcpseq_partition_w = Window.partitionBy($"dst", $"tcpseq").orderBy($"timestamp".asc)
    val sorted_w = Window.orderBy($"timestamp".asc)
    val jt_sorted_w = Window.orderBy($"jobid".asc, $"taskid".asc)
    val taskid_w = Window.partitionBy($"jobid").orderBy($"timestamp".asc)

    // assume all packets are seen first at the src
    val src_pcap = pcapReader.readPcap(lspark, srcfile)
        .filter("proto=6").filter("tcpseq is not null")  //.filter("dack is not null")
    val dst_pcap = pcapReader.readPcap(lspark, dstfile)
        .filter("proto=6").filter("tcpseq is not null")   //.filter("dack is not null").persist(MEMORY_AND_DISK_SER);
    
    val jobid_index = src_pcap.withColumn("rn", row_number.over(tcpseq_partition_w)).where("rn=1").drop("rn")
        .withColumn("jobid", row_number.over(sorted_w))
        .drop("timestamp", "framenumber", "proto", "framelen", "src", "srcport", "dstport", "dsn", "dack")
    
    // now we have to join the job IDs back onto the source_pcap
    // and compute the task IDs
    val jsrc_pcap = src_pcap.as("src").join(jobid_index.as("job"), $"src.dst"===$"job.dst" && $"src.tcpseq"===$"job.tcpseq", "left_outer")  // && $"src.dack"===$"job.dack", "left_outer")
        .withColumn("taskid", row_number.over(taskid_w))
        .withColumn("tasknum", row_number.over(jt_sorted_w))
        .drop($"job.dst").drop($"job.tcpseq").drop($"job.tcpack")
        .orderBy("tasknum")
    
    // now we finally have a table of jobid, taskid and associated fields
    //val taskid_index = jsrc_pcap.select("jobid", "taskid", "tasknum", "dst", "dstport", "dsn", "dack", "tcpseq", "tcpack")
    
    // finally, join the src and dst pcaps into one big dataset.
    // this can be used to see the packets dropped (dst.timestamp will be null), or latency.
    val jpcap = jsrc_pcap.as("src")
        .join(dst_pcap.as("dst"), $"dst.dst"===$"src.dst" && $"dst.dstport"===$"src.dstport" && $"dst.tcpseq"===$"src.tcpseq" && $"dst.tcpack"===$"src.tcpack", "left_outer")
        .drop($"dst.src").drop($"dst.srcport").drop($"dst.dst").drop($"dst.dstport").drop($"dst.proto").drop($"dst.framelen")
        //.drop($"dst.tcpseq").drop($"dst.tcpack")
        .drop($"dst.dsn").drop($"dst.dack").drop($"dst.framenumber").drop($"src.framenumber")
        .orderBy("tasknum")
    
    return jpcap
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
   * The timestamps are translated so that the earliest src frame appears at time 0.0
   * 
   * Example return value:
   * Array(Map(ip -> "dst: 10.1.2.2 -> 10.1.1.2", pktnum -> 5129, t -> 0.051397085189819336), 
   *       Map(ip -> "src: 10.1.2.2 -> 10.1.1.2", pktnum -> 5129, t -> 0.0), 
   *       Map(ip -> "dst: 10.1.3.2 -> 10.1.1.2", pktnum -> 5130, t -> 0.15339207649230957), 
   *       Map(ip -> "src: 10.1.3.2 -> 10.1.1.2", pktnum -> 5130, t -> 0.10199308395385742), 
   *       Map(ip -> "dst: 10.1.3.2 -> 10.1.1.2", pktnum -> 5131, t -> 0.2965109348297119))
   */
  def experimentPaths(jpcap:Dataset[Row], start:Int, end:Int): Array[Map[String,Any]] = {
    
    val tmp = jpcap.filter($"jobid" >= start && $"jobid" <= end).persist();
    
    val start_time = jpcap.select(min("src.timestamp")).first.getDouble(0)
    
    val result =  tmp.filter($"dst.timestamp".isNotNull).select(concat_ws(" ", lit("dst:"), $"src", lit("->"), $"dst"), $"tasknum", ($"dst.timestamp"-lit(start_time)).as("timestamp"))
            .union(tmp.select(concat_ws(" ", lit("src:"), $"src", lit("->"), $"dst"), $"tasknum", ($"src.timestamp"-lit(start_time)).as("timestamp"))).orderBy("tasknum")
            .collect().map( x => Map("ip"->x.getString(0), "pktnum"->x.getInt(1), "t"->x.getDouble(2)))

    tmp.unpersist()

    if (result.length == 0) {
        println("ERROR: experimentPaths resulted in empty dataset")
    }

    return result
  }
  
  
  /**
   * Same as the experimentPaths() function above, except it takes time values as the
   * start/end points.  Note that this will select the MP packets that first appear in
   * this time range, so the actual range of times in the resulting data may be a bit longer.
   * 
   * This function just computes the range of job IDs that have their start within
   * this time range, and then calls the normal experimentPaths() function.
   * 
   * Especially when called with strict_bound=true, this version is more complex than
   * the normal experimentPaths function.
   * 
   * TODO: this will throw an exception if invalid time values are passed in.  Fix that!
   */
  def experimentPathsTime(jpcap:Dataset[Row], start:Double, end:Double, strict_bound:Boolean=false): Array[Map[String,Any]] = {
    
    val start_time = jpcap.select(min("src.timestamp")).first.getDouble(0)
    var start_job = 0
    var end_job = 0
    
    if (strict_bound) {
      // getting the start time is easy
       start_job = jpcap.filter("taskid=1")
        .filter($"src.timestamp" >= (start+start_time) && $"src.timestamp" <= (end+start_time))
        .select(min("jobid"))
        .first.getInt(0)
        
        // get job id of the last job that completes entirely within the time bounds
        end_job = jpcap
            .withColumn("maxtime", greatest("src.timestamp","dst.timestamp") - start_time)
            .filter($"maxtime" > end)
            .select(min("jobid")).first.getInt(0) - 1

    } else {
      val job_bounds = jpcap.filter("taskid=1")
        .filter($"src.timestamp" >= (start+start_time) && $"src.timestamp" <= (end+start_time))
        .select(min("jobid"), max("jobid"))
        .first
    
      start_job = job_bounds.getInt(0)
      end_job = job_bounds.getInt(1)
    }
    
    if (start_job >= end_job) {
        println("WARNING: experimentPathsTime() - time bounds contain no data points")
        return Array()
    }

    return experimentPaths(jpcap, start_job, end_job)
  }

  
  /**
   * Compute the throughput in terms of bytes and number of packets over a sliding time window.
   * 
   * The throughput is measured at the dst as the first reception of each packet.  Each packet may
   * be received multiple times if there are resends, but the throughput is only computed for the
   * first reception.  The second return value is the redundant throughput, which is the rate data
   * is being received in frames with rxnum > 1.
   * 
   * The function now returns an array of data points suitable for plotting with Vegas-viz.
   * 
   * This computes throughput at the reception time of each packet, which isn't ideal; if there
   * are no packets arriving, then there are no data points, but when there are lots of packets
   * arriving, then there many many essentially redundant datapoints.  This will especially be an
   * issue for computing redundant throughput, which will be about zero most of the time for 
   * most schedulers, resulting in no datapoints.
   * 
   * See the rollingThroughput() function below for something a little more regular.
   * 
   * TODO: it may be better to compute the throughput at regular intervals, say every 0.1s.
   */
  def throughput(jpcap:Dataset[Row], windowSize:Double, destination:String, start:Double, end:Double, label:String): Array[Map[String,Any]] = {
      val job_partition_w = Window.partitionBy($"jobid").orderBy($"dst.timestamp".asc)
      
      val datascale:Double = 1.0e6
      
      val windowSize_l:Long = (windowSize*datascale).toLong
      val throughput_factor = 1.0 / windowSize
      
      // it seems like the window orderBy() column can't be a Double
      val sliding_tp_w = Window.orderBy($"ts").rangeBetween(0, windowSize_l)  //Window.currentRow,(windowSize*1.0e12).toLong)
      
      val start_l:Long = (start * datascale).toLong
      val end_l:Long = (end * datascale).toLong
      
      val trace_start_time = jpcap.select(min("src.timestamp")).first.getDouble(0)
      val start_epoch:Double = start + trace_start_time
      val end_epoch:Double = end + trace_start_time
      
      val rxtrace = jpcap.filter("dst.timestamp is not null")
          .withColumn("rxnum", row_number.over(job_partition_w))
          .filter($"dst.timestamp" >= start_epoch && $"dst.timestamp" <= end_epoch)
          .withColumn("tsd", $"dst.timestamp" - trace_start_time)
          .withColumn("ts", (($"dst.timestamp" - trace_start_time)*datascale).cast(LongType))
          .orderBy("ts")
          .persist(MEMORY_AND_DISK)
                
      // now we can get the first reception of each packet
      val tput = rxtrace.filter("rxnum = 1")
          .withColumn("throughput", sum("framelen").over(sliding_tp_w) * throughput_factor)
          .withColumn("pktcount", sum(lit(1)).over(sliding_tp_w))
      
      // we can get the redundant recptions
      val redundant_tput = rxtrace.filter("rxnum > 1")
          .withColumn("throughput", sum("framelen").over(sliding_tp_w) * throughput_factor)
          .withColumn("pktcount", sum(lit(1)).over(sliding_tp_w))

      val tp_label = label + " throughput"
      val tpr_label = label + " redundant"

      val tpdata = tput.select(lit(tp_label), $"tsd", $"throughput", $"pktcount")
        .union(redundant_tput.select(lit(tpr_label), $"tsd", $"throughput", $"pktcount")).orderBy("tsd")
        .collect().map( x => Map("title"->x.getString(0), "x"->x.getDouble(1), "y"->x.getDouble(2), "pktcount"->x.getLong(3)))
        
      rxtrace.unpersist()
      
      return tpdata
  }
  
  
  /**
   * Another way to compute the throughput and redundant throughput over some time period.
   * This method uses rolling/sliding windows of fixed increments.  The benefit of this over
   * the method above is that you get evenly spaced data points.  Less datapoints in the
   * regions with lots of packets, and more data points in the regions with few or no packets.
   * 
   * One part I find klunky is that the column we do the window over must be TimestampType.
   * Therefore we cast the timestamps to that type, which gives us timestamps in 1970, do
   * the windowing, and then convert them back to double.
   * 
   * You must specify both:
   * windowSize = the width of the samples
   * increment = the amount to move the window by
   * 
   * The the time values returned are the start boundaries of the windows.
   * The smallest precision for windows and increments is 1microsecond.
   * 
   */
  def rollingThroughput(jpcap:Dataset[Row], windowSize:Double, destination:String, start:Double, end:Double, incr:Double, label:String): Array[Map[String,Any]] = {
      val job_partition_w = Window.partitionBy($"jobid").orderBy($"dst.timestamp".asc)

      if (windowSize < 1e-6) {
        println("ERROR: rollingThroughput - window size smallest unit is milliseconds")
        return Array()
      }
      
      if (incr < 1e-6) {
        println("ERROR: rollingThroughput - increment size smallest unit is milliseconds")
        return Array()
      }
      
      // smallest window interval string is microseconds, so we have to convert to that
      val windowSize_us = (windowSize * 1.0e6).toLong
      val incr_us = (incr * 1.0e6).toLong
      
      // when computing throughput we have to account for the fact that the samples
      // are taken over windows smaller than 1 second.
      val throughput_factor = 1.0 / windowSize
            
      val trace_start_time = jpcap.select(min("src.timestamp")).first.getDouble(0)
      val start_epoch:Double = start + trace_start_time
      val end_epoch:Double = end + trace_start_time
      
      val rxtrace = jpcap.filter("dst.timestamp is not null")
          .withColumn("rxnum", row_number.over(job_partition_w))
          .filter($"dst.timestamp" >= start_epoch && $"dst.timestamp" <= end_epoch)
          .withColumn("tsd", $"dst.timestamp" - trace_start_time)
          .withColumn("tsts", $"tsd".cast(TimestampType))
          .orderBy("tsts")
          .persist(MEMORY_AND_DISK)
      
      // now we can get the first reception of each packet
      val tput = rxtrace.filter("rxnum = 1")
          .groupBy(window($"tsts", windowSize_us+ " microseconds", incr_us+ " microseconds"))
          .agg((sum("framelen")*throughput_factor).as("throughput"), (sum(lit(1))).as("pktcount"))
          .withColumn("ws", $"window.start".cast(DoubleType))
          .orderBy("ws")
               
      // we can get the redundant recptions
      val redundant_tput = rxtrace.filter("rxnum > 1")
          .groupBy(window($"tsts", windowSize_us+ " microseconds", incr_us+ " microseconds"))
          .agg((sum("framelen")*throughput_factor).as("throughput"), (sum(lit(1))).as("pktcount"))
          .withColumn("ws", $"window.start".cast(DoubleType))
          .orderBy("ws")
      
      val tp_label = label + " throughput"
      val tpr_label = label + " redundant"
      
      val tpdata = tput.select(lit(tp_label), $"ws", $"throughput", $"pktcount")
        .union(redundant_tput.select(lit(tpr_label), $"ws", $"throughput", $"pktcount")).orderBy("ws")
        .filter("ws >= 0.0")
        .collect().map( x => Map("title"->x.getString(0), "x"->x.getDouble(1), "y"->x.getDouble(2), "pktcount"->x.getLong(3)))
        
      rxtrace.unpersist()
      
      return tpdata
  }
  
  
  /**
   * compute the interval between successive packets at the src and dst
   * 
   * NOTE: if the jpcap passed in has the smaller frames filtered out, then 
   *       the resulting IPSs will have some invalid datapoints.
   */
  def interpacketTimes(jpcap:Dataset[Row]): (Dataset[Row],Dataset[Row]) = {
    val timeorder_src_w = Window.partitionBy("src").orderBy("timestamp")
    val timeorder_dst_w = Window.partitionBy("dst").orderBy("timestamp")
    
    // extract the src IPTs
    val src_ipt = jpcap.select($"src.timestamp".alias("timestamp"), $"framelen", $"src")
      .withColumn("ipt", $"timestamp" - lag($"timestamp",1).over(timeorder_src_w))

    // extract the dst IPTs
    val dst_ipt = jpcap.filter("dst.timestamp is not null")
      .select($"dst.timestamp".alias("timestamp"), $"framelen", $"dst")
      .withColumn("ipt", $"timestamp" - lag($"timestamp",1).over(timeorder_dst_w))
    
    return (src_ipt.filter("ipt is not null"), dst_ipt.filter("ipt is not null"))
  }
  
  
  /**
   * Estimate the pdf of latency data using mllib's KernelDensity class.
   * 
   * XXX: we do two operations on the data Dataset, and who knows what goes on inside KernelDensity.
   *      Should we persist/unpersist it?  Te issue would be if it's already persisted, we don't want
   *      to unpersist it.
   */
  def genericPdf(data:Dataset[Row], colname:String, numpoints:Double): (Array[Double], Array[Double]) = {
      
      val is_cached = data.rdd.getStorageLevel.useMemory || data.rdd.getStorageLevel.useDisk || data.rdd.getStorageLevel.useOffHeap
      if (! is_cached) {
        data.persist()
      }
		  val rtmp = data.filter(colname+" is not null").select(min(colname),max(colname)).first;
		  val (lmin, lmax) = (rtmp.getDouble(0), rtmp.getDouble(1));
		  val evalpoints = lmin to lmax by ((lmax-lmin)/numpoints) toArray;
		  val bandwidth = ((lmax-lmin)/numpoints);

		  val kd = new KernelDensity()
				  .setSample(data.select(colname).map( x => x.getDouble(0) ).rdd)
				  .setBandwidth(bandwidth);

		  // Find density estimates for the given values
		  val densities = kd.estimate(evalpoints);

		  if (! is_cached) {
        data.unpersist()
      }

		  //return evalpoints.zip(densities)
		  return (evalpoints, densities)
  }

  /**
   * Given a latency dataset as returned by loadProcessPcapLatency(), trim off the specified
   * number of seconds from the start and end of the data.
   * 
   * The intention is to remove the transient slow-start part of te connection, and any
   * other artifacts experienced at the end of the flow.
   */
  def trim(data:Dataset[Row], startTrim:Double, endTrim:Double): Dataset[Row] = {
      val mm = data.agg(min("src_timestamp")).first.getDouble(0);
      val mx = data.agg(max("src_timestamp")).first.getDouble(0);
      return data.filter($"src_timestamp" > (mm+startTrim) && $"src_timestamp" < (mx-endTrim));
  }
  
  
  /**
   * In a given trace we want to ask:
   * - what fraction of the segments are first delivered over each subflow?
   * - what fraction of the segments sent on both subflows are delivered first
   *   over each subflow?
   * - what fraction of the segments lost on one subflow have their first
   *   delivery over the other subflow?
   * - what fraction of the segments on each subflow are lost?
   * - of the segments sent over a subflow, what fraction were resends of lost
   *   packets on the other subflow?
   *   
   * This should only be run on individual pairs of src/dst files.  Otherwise
   * the DSNs can wrap and you can't work with them.
   */
  def lossStatistics(srcfile:String, dstfile:String) {
    val src_data = pcapReader.readPcap(lspark, srcfile)
      .filter("proto=6").filter("dsn is not null").filter("dack is not null").filter("framelen > 100")
    val dst_data = pcapReader.readPcap(lspark, dstfile)
      .filter("proto=6").filter("dsn is not null").filter("dack is not null").filter("framelen > 100")
    
    val packet_ts_w = Window.partitionBy($"dsn",$"tcpseq").orderBy($"timestamp".asc)

    // join the full send data to full receive data
    // this is a left outer join because the packets may get lost
    // we use the "duplicate_tx_order" steps to fix extremely rare segments where a TX event matches to multiple RX events
    // in these cases we keep the row were the RX event matches with the earlier of TX events
    val txrx = src_data
      .withColumn("next_ts", lead("timestamp", 1, Double.PositiveInfinity).over(packet_ts_w)).as("src")
      .join(dst_data.withColumnRenamed("timestamp","rx_timestamp").withColumnRenamed("framenumber","rx_framenumber").as("dst"),
            $"src.dsn"===$"dst.dsn" && $"src.src"===$"dst.src" && $"src.dst"===$"dst.dst" && $"src.timestamp"<$"rx_timestamp" && $"src.next_ts">$"rx_timestamp" && ($"rx_timestamp"-$"src.timestamp")<1.0,
            "left_outer")
      .drop($"dst.src").drop($"dst.dst").drop($"dst.srcport").drop($"dst.dstport").drop($"dst.proto").drop($"dst.framelen").drop($"dst.dack").drop($"dst.dsn").drop($"dst.tcpseq").drop($"dst.tcpack")
      .na.fill(Double.PositiveInfinity, Seq("rx_timestamp"))
      .withColumn("duplicate_tx_order", count("*").over(Window.partitionBy($"timestamp",$"framenumber").orderBy($"timestamp")))
      .filter("duplicate_tx_order=1").drop("duplicate_tx_order")
      .withColumn("rx_order", row_number.over(Window.partitionBy("dsn").orderBy("rx_timestamp")))
      .withColumn("tx_order", row_number.over(Window.partitionBy("dsn").orderBy("timestamp")))
      .withColumn("latency",$"rx_timestamp"-$"timestamp")
      .orderBy($"dsn".asc,$"timestamp".asc)
      .persist()
      //.withColumn("seg_id",row_number.over(Window.orderBy($"src.dsn".asc,$"src.timestamp".asc)))
    
    // figure out which DSNs got sent more than once
    val dup_dsn = src_data.groupBy("dsn").count.filter("count>1").persist
    
    // get a list of all DSNs that experience at least one loss
    val drop_dsns = txrx.filter($"rx_framenumber".isNull).select($"dsn").distinct.orderBy("dsn").persist
    println("all losses = "+txrx.join(drop_dsns,"dsn").select($"dsn").distinct.count)
    
    // get a list of all DSNs that experience a loss on the long fat subflow
    val drop_lfs_dsns = txrx.filter($"rx_framenumber".isNull).filter("src.src='10.1.2.2'").select($"dsn").distinct.orderBy("dsn").persist
    println("lfs losses = "+txrx.join(drop_lfs_dsns,"dsn").select($"dsn").distinct.count)
    
    // get a list of the DSNs that are sent on both subflows
    val tx_both_subflows_dsns = txrx.withColumn("maxseq",max($"src.tcpseq").over(Window.partitionBy($"src.dsn")))
      .withColumn("minseq",min($"src.tcpseq").over(Window.partitionBy($"src.dsn")))
      .filter("maxseq != minseq")
      .select($"dsn").distinct.orderBy($"dsn".asc)
      .persist
    
    // the segments that are sent on both subflows and experience a loss on the long fat subflow
    /*
     * Here we start to see segments that are lost on the long fat subflow, resent on the
     * short skinny subflow, and delivered first over the short skinny subflow.  Because the
     * duplicate ACKs are already in flight, they are generally also resent over the long fat
     * subflow too.
     * In some cases we see that the long fat subflow resend takes place after the segment
     * is already received over the short skinny one.
     * We would like to know what fraction of the segments lost over the long fat subflow
     * end up being delivered first over the short skinny subflow.
     */
    val sss_loss_corrections = txrx.join(drop_lfs_dsns,"dsn").join(tx_both_subflows_dsns,"dsn")
      .orderBy($"dsn".asc,$"timestamp".asc)
      .filter("rx_order=1 AND src='10.1.3.2'")
      .select($"dsn").distinct
      .count
    println("sss_loss_corrections = "+sss_loss_corrections)
    
    // segments sent over sss
    println("segments sent over sss: "+txrx.filter("src='10.1.3.2'").select($"dsn").distinct.count)

    // segments sent over sss arriving first
    println("segments sent over sss arriving first: "+txrx.filter("rx_order=1 AND src='10.1.3.2'").select($"dsn").distinct.count)

    // segments sent over lfs
    println("segments sent over lfs: "+txrx.filter("src='10.1.2.2'").select($"dsn").distinct.count)

    // segments sent over lfs arriving first
    println("segments sent over lfs arriving first: "+txrx.filter("rx_order=1 AND src='10.1.2.2'").select($"dsn").distinct.count)
    
    // how often is a segment sent over the sss first?
    println("segments sent first over sss: "+txrx.filter("tx_order=1 AND src='10.1.3.2'").select($"dsn").distinct.count)
    
    // would like to know the number of segments sent first on the lfs, but arriving first on the sss
    val lfs_txfirst_dsns = txrx.filter("tx_order=1 AND src='10.1.2.2'").select($"dsn").distinct.persist
    println("segments sent first over lfs, arriving first over sss: "+txrx.filter("rx_order=1 AND src='10.1.3.2'").join(lfs_txfirst_dsns,"dsn").select($"dsn").distinct.count)
    
    txrx.unpersist()
  }
  
  
}


