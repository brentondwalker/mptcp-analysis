import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._ // for `when`
import org.apache.spark.sql.expressions.Window
import org.apache.spark.api.java.StorageLevels._

/**
 * This will be code to turn job/task event data into a table of
 * job/task data with arrive/start/end times and total durations computed
 */

object PcapProcessor {

  /**
   * Load the src and dst pcap files and pick out the first time where each
   * MPTCP packet that is:
   * 1. sent
   * 2. recieved
   * 
   * This lets us compute the latency.
   * 
   * The loadProcessPcapFull() function below does all this and more, so this
   * function may be both buggy and obsolete.  However it should be faster than
   * loadProcessPcapFull() because it takes a coarser grouping of the frames
   * and doesn't do as much processing.
   */
  def loadProcessPcapLatency(spark:SparkSession, srcfile:String, dstfile:String): Dataset[Row] = {
    import spark.implicits._
    
    // XXX - based on what we know now, grouping by dack and dstport is probably a mistake
    //       those fields will be different for packets sent over the different links
    val first_w = Window.partitionBy($"proto", $"dstport", $"dsn", $"dack").orderBy($"timestamp".asc)
    
    // read the src and dst pcaps
    // We are only interested in TCP traffic, so filter for proto=6 right away
    // Also we filter out rows with no DSN.  The missing DSN happens rarely, but
    // Vu says it's a legitimate omission.
    // TODO: deal with missing DSN, or show that we can discard those frames.
    val src_pcap = pcapReader.readPcap(spark, "/home/ikt/short-no-outage/iperf-interupted-lowrtt-on10-off3-sender-1.csv")
        .filter("proto=6").filter("dsn is not null").filter("dack is not null")
        .withColumn("rn", row_number.over(first_w)).where("rn=1").drop("rn");
    val dst_pcap = pcapReader.readPcap(spark, "/home/ikt/short-no-outage/iperf-interupted-lowrtt-on10-off3-receiver-1.csv")
        .filter("proto=6").filter("dsn is not null").filter("dack is not null")
        .withColumn("rn", row_number.over(first_w)).where("rn=1").drop("rn");
    
    // we join the two pcaps based on proto, dst, dstport, dsn, dack.
    // XXX - based on what we know now, I think this is a mistake.
    return src_pcap.as("src").join(dst_pcap.as("dst"), $"src.proto"===$"dst.proto" && $"src.dst"===$"dst.dst" && $"src.dstport"===$"dst.dstport" && $"src.dsn"===$"dst.dsn" && $"src.dack"===$"dst.dack")
    .drop("proto").drop($"dst.src").drop("srcport").drop($"dst.dst").drop("dstport").drop($"dst.framelen").drop($"dst.dsn").drop($"dst.dack")
    .withColumn("latency", $"dst.timestamp" - $"src.timestamp")
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
   */
  def loadProcessPcapFull(spark:SparkSession, srcfile:String, dstfile:String): (Dataset[Row],Dataset[Row],Dataset[Row]) = {
    import spark.implicits._

    val dsn_partition_w = Window.partitionBy($"dst", $"dsn").orderBy($"timestamp".asc)
    val sorted_w = Window.orderBy($"timestamp".asc)
    val jt_sorted_w = Window.orderBy($"timestamp".asc)
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
  
}


