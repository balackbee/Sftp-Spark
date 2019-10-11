import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
val spark=SparkSession.builder().appName("Test WORKSHEET").getOrCreate()
val schemaString= "order_id:int order_date:string order_customer_id:int order_status:string"
val a=schemaString.split(" ")
/*val fields =a.map(f=> {
  if(f.split(":")(1)=="int")
    StructField(f.split(":")(0),IntegerType)
  else
    StructField(f.split(":")(1),StringType)
})*/
val fields=a.map(f=> f.split(":")(1)match {
  case "int"=> StructField(f.split(":")(0),IntegerType)
  case _ => StructField(f.split(":")(0),StringType)
})
val schemaa=StructType(fields)
//import spark.implicits._
//val data =spark.read.textFile("C:\\Users\\uprakash\\Desktop\\seq_text.txt")
val d1=Seq(Row(10,"05-05-2019",1501,"Dispached"),Row(11,"06-05-2019",1502,"Deliverd"))
//  val l1: List[String]=List("a","b","c","d")
val df=spark.createDataFrame(spark.sparkContext.parallelize(d1),schemaa)
df.show()
println("=====END=====")


