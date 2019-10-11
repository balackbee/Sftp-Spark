import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkSQLTest {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka")setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
val spark= SparkSession.builder().appName("SPARK SQL TEST ").master("local").getOrCreate()

   // Path of ur private key to access the file server
   val path = "C:\\Users\\uprakash\\Desktop\\Keys\\id_rsa"
   println(path)

   // reading A file from the SFTP server
   val df=spark.read.format("com.springml.spark.sftp")
     .option("host","10.222.113.125")
     .option("username","uprakash")
     .option("pem",s"$path")
     .option("filetype","csv")
     .option("header",true)
     .option("inferschema",true)
     .load("/uprakash/testCsv_file.csv")

   println("DF TEST CSV data data")
   println("THIS IS fuULL  DF")

   // All The elementary Operations or business requirement will be done here
   //   Transformation
   //   Action

   df.show(10,false)
   df.printSchema()

   df.write.format("jdbc").mode("overwrite")
     .option("driver","org.postgresql.Driver")
     .option("url","jdbc:postgresql://localhost:5432/sampledb")
         .option("dbtable","newTable")
         .option("user","postgres")
         .option("password","postgres")
     .option("inferschema",true)
     .option("header",true)
     .save()
   //import spark.implicits._
 }
}
