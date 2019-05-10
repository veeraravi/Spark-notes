import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import com.typesafe.config.ConfigFactory

object DataFrameEx19 {
case class Orders(
order_id: Int,
order_date: String,
order_customer_id: Int,
order_status: String)

case class OrderItems(
order_item_id: Int,
order_item_order_id: Int,
order_item_product_id: Int,
order_item_quantity: Int,
order_item_subtotal: Float,
order_item_price: Float

)
def main(args: Array[String]): Unit = {
val appConf=ConfigFactory.load()
val conf = new SparkConf().setAppName("spark exercise 17")
.setMaster(appConf.getConfig(args(2)).getString("deployment"))
val sc = new SparkContext(conf)
val inputPath = args(0)
val outputPath = args(1)
val fs = FileSystem.get(sc.hadoopConfiguration)
val inputPathExists = fs.exists(new Path(inputPath))
val outputPathExists = fs.exists(new Path(outputPath))
if (!inputPathExists) {
println("Input Path does not exists")
return
}

if (outputPathExists) {
  fs.delete(new Path(outputPath), true)
}
val sqlContext = new SQLContext(sc)

val inputPath="/public/retail_db"
val outputPath="/user/ranadev591/1"

import sqlContext.implicits._
val ordersDF = sc.textFile(inputPath + "/orders").
map { x => {val y=x.split(",")
  Orders(y(0).toInt,y(1),y(2).toInt,y(3))} }.toDF()
  
   val ordersItemsDF = sc.textFile(inputPath + "/order_items").
map { x => {val y=x.split(",")
  OrderItems(y(0).toInt,y(1).toInt,y(2).toInt,y(3).toInt,y(4).toFloat,y(5).toFloat)} }.toDF()
 
  val ordersFiltered=ordersDF.filter(ordersDF("order_status")==="COMPLETE")
  val ordersjoin=ordersFiltered.join(ordersItemsDF,ordersFiltered("order_id")=== ordersItemsDF("order_item_order_id"))
  ordersjoin.groupBy("order_date").sum(("order_item_subtotal")).sort("order_date").rdd.saveAsTextFile(outputPath)
  
  ordersjoin.groupBy("order_date").sum(("order_item_subtotal")).sort("order_date").coalesce(1).write.format("csv").option("header","true").option("delimiter","|").save(outputPath)
  
 ordersjoin.groupBy("order_date").sum(("order_item_subtotal")).sort("order_date").coalesce(1).write.option("header", "true").option("delimiter","|").csv(outputPath)

 
 
 val outputPath="/user/veera/1"
 val filename="result.csv"
 val outputfilename=outputPath+"/temp_"+filename
 val mergedfile=outputPath+"/merged_"+filename
 
 val mergedfineglob =outputfilename 
 
 
 def mergeTextFiles(srcPath: String, dstPath: String, deleteSource: Boolean): Unit =  {
  import org.apache.hadoop.fs.FileUtil
  import java.net.URI
  val config = spark.sparkContext.hadoopConfiguration
  val fs: FileSystem = FileSystem.get(new URI(srcPath), config)
  FileUtil.copyMerge(
    fs, new Path(srcPath), fs, new Path(dstPath), deleteSource, config, null
  )
   
}
ordersjoin.groupBy("order_date").sum(("order_item_subtotal")).sort("order_date").coalesce(1).write.option("header", "true").option("delimiter","|").csv(outputfilename)
 mergeTextFiles(mergedfineglob,mergedfile,true)
}
}
