import org.apache.spark.sql._
import org.apache.spark._

object sparkup {

 def main(args: Array[String]): Unit = {

  sessionSpark()
 }



def sessionSpark():Unit={
 System.setProperty("winutils","C:\\hadoop")
  val ss= SparkSession.builder()
    .master(master = "local[*]")
    .appName(name = "Ma premiere application")
    //.enableHiveSupport()
    .getOrCreate()
 val rdd1 =ss.sparkContext.parallelize(Seq("ma premieere seesion en spark"))
  rdd1.foreach(l=>println(l))
 val df_1=ss.read
   .format(source="csv")
   .option("inferSchema","true")
   .option("header","true")
   .option("delimiter",";")
   .csv(path = "C:\\Users\\Marius\\Desktop\\projet\\orders_csv.csv")
    df_1.show( numRows =10)
    df_1.createOrReplaceTempView(viewName = "orders")
    ss.sql(sqlText = "SELECT * FROM orders WHERE city= 'NEWTON'").show()

}


}
