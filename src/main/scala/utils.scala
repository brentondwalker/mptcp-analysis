import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * Some utility functions that will be convenient to factor out
 * because the code is re-used all over.  This way we can change it
 * in one place.
 */
object utils {

  def createSparkSession(appName: String): SparkSession = {
    val spark = SparkSession.builder
      .appName("gtrace-analysis")
      .config("LogLevel", "WARN")
      .getOrCreate()
      
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
      
      return spark
  }

    def createSparkSession(appName: String, master: String): SparkSession = {
    val spark = SparkSession.builder
      .appName("gtrace-analysis")
      .master(master)
      .config("LogLevel", "WARN")
      .getOrCreate()
      
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
      
      return spark
  }

  
  
  
}


