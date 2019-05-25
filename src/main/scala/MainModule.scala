import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object MainModule {

  def main(args: Array[String]): Unit = {



      val file = "/Users/dhirav/D2Project/GameAnalytics/events.json.gz"

    val dir = "/Users/dhirav/D2Project/GameAnalytics/"



      val sparkSession = SparkSession.builder
        .master("local")
        .appName("QuantexaTask")
        .config("spark.sql.warehouse.dir", dir)
        .getOrCreate()

    import sparkSession.implicits._

      val df = sparkSession.read.json(file)

    df.printSchema()

     //df.select("data").limit(100).show(100)

     //df.withColumn("data",explode(df("data"))).as("d").select("d")

    val ndf = df.select($"data",explode($"data.user_id").as("rec"))

    ndf.show(100)


    }


}
