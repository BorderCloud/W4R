import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


trait SparkEnvironnement {
  private val master = "local[*]"
  private val appName = "FindIdWikidataWithFewData"
  private val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")

  val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = ss.sparkContext
  def sparkEnvironnementInfo(): String ={
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    return "Spark version : " + sc.version + "\n" +
      "Scala version : " + util.Properties.versionString +
      "** Used Memory:  " + ((runtime.totalMemory - runtime.freeMemory) / mb).toString() + "\n" +
      "** Free Memory:  " + (runtime.freeMemory / mb).toString() + "\n" +
      "** Total Memory: " + (runtime.totalMemory / mb).toString() + "\n" +
      "** Max Memory:   " + (runtime.maxMemory / mb).toString()
  }
}
