import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.


val inputBSV = spark.read.option("header","true").option("delimiter", "|").option("inferSchema", "true").csv("input.bsv")

/* 2) Create a spark script called unpivot.iscala that uses input.bsv, transposes the columns, and answers the following: */

  /* a) What is the smallest value per column? */
  /* b) What is the biggest value per column? */
  /* c) What is the average per column? */
  /* d) What is the number of unique values per column? */
  /* e) How many values are null per column? */
  /* f) Write the file out to BSV back on HDFS and call it output.bsv */

/*   ** You're output should look something like this -
    +----------+------------------+---+---+-----+----------+--------+
    |ColumnName|               avg|min|max|  sum|numUniques|numNulls|
    +----------+------------------+---+---+-----+----------+--------+
    |         A|              10.8| -5| 25| 54.0|         5|       1|
    |         B|             15.75| 14|  8| 63.0|         4|       2|
    |         C|41.333333333333336|100|  9|124.0|         3|       3|
    |         D|              22.4|-10| 55|112.0|         5|       1|
    |         E|              53.0|101|  5|265.0|         5|       1|
    |         F|              40.6| 12| 90|203.0|         5|       1|
    +----------+------------------+---+---+-----+----------+--------+
   ** This means you'll have to unpivot the columns from
              A|B|C|D|E -> A|1
              1|2|3|4|5    B|2
                           C|3
                           D|4
                           E|5
   or Key,Value pairs once you've done you're calculations and add a tag that describes what
   each value represents.
*/
//create the schema for the dataframe and then calculate the values for each row 
/*val schema = StructType (
  List(
    StructField("columnName", StringType, true),
    StructField("avg", DoubleType, true),
    StructField("min", IntegerType, true),
    StructField("max", IntegerType, true),
    StructField("sum", DoubleType, true),
    StructField("numUniques", IntegerType, true),
    StructField("numNulls", IntegerType, true)
  )
)

val headers = inputBSV.columns

val inputData = headers.map(x => Row(x,
    inputBSV.agg(avg(x)).head().mkString.toDouble,
    inputBSV.agg(min(x)).head().mkString.toInt,
    inputBSV.agg(max(x)).head().mkString.toInt,
    inputBSV.agg(sum(x)).head().mkString.toDouble,
    inputBSV.agg(countDistinct(x)).head().mkString.toInt,
    inputBSV.filter(inputBSV(x).isNull).count().toInt
    )
    )
    

val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(inputData),StructType(schema)

inputDF.write.mode("overwrite").format("csv").option("delimiter", "|").save("output.bsv")
*/
