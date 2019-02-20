/*

--Summary--------


This exercise will focus on testing your ability to create and automate a workflow using
Drake once you have created a few spark scripts.


--SPARK---------

*/

/* 1) Put the input.bsv file up onto HDFS. */
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

val inputBSV = spark.read.option("header","true").option("delimiter", "|").option("inferSchema", "true").csv("/Users/latifzx/Documents/ScalaPrac/input.bsv")

/* 2) Create a spark script called unpivot.scala that uses input.bsv, transposes the columns, and answers the following: */

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
val schema = StructType (
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
/*val inputData = Seq(
  Row("A",
    inputBSV.agg(avg("A")).head().getDouble(0), 
    inputBSV.agg(min("A")).head().mkString.toInt, 
    inputBSV.agg(max("A")).head().mkString.toInt, 
    inputBSV.agg(sum("A")).head().getDouble(0), 
    inputBSV.agg(countDistinct("A")).head().mkString.toInt, 
    inputBSV.filter(inputBSV("A").isNull).count().toInt
    )
    )
*/

val inputData = headers.map(x => Row(x, 
    inputBSV.agg(avg(x)).head().mkString.toDouble, 
    inputBSV.agg(min(x)).head().mkString.toInt, 
    inputBSV.agg(max(x)).head().mkString.toInt, 
    inputBSV.agg(sum(x)).head().mkString.toDouble, 
    inputBSV.agg(countDistinct(x)).head().mkString.toInt, 
    inputBSV.filter(inputBSV(x).isNull).count().toInt
    )
    )



//.toDF("columnName", "avg", "min", "max", "sum", "numUniques", "numNulls")

//val inputDF = sc.parallelize(inputData)
val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(inputData),StructType(schema))



/* 3) Create a second spark script called selectRename.scala that also uses input.bsv and does the following: */
   /* a) Rename all of the columns like so: A -> A_1, B -> B_2, etc. */
   /* b) Drop all columns that have an extension > 3 */
   /* c) Write the file out BSV on HDFS and call it output1.bsv */
val newHeaders = Seq("A_1", "B_2", "C_3", "D_4", "E_5", "F_6")
val renamedDF =  inputBSV.toDF(newHeaders: _*)
val dropDF = renamedDF.drop("D_4", "E_5", "F_6")






/* 4) Create a third spark script called globalStats.scala that uses output.bsv as its input and answers the following question: */
  /* a) What is the global average across all of the columns (average of the all the values per metric)? */
  /* b) What is the unique number of columns? */
  /* c) Write the file out to BSV on HDFS and call it output2.bsv */

/*
  ** You're output should look something like this -
  +-------------------+------------------+------------------+------+------------------+-------------+-----------+
  |distinctColumnNames|            avgAvg|            avgMin|avgMax|            avgSum|avgNumUniques|avgNumNulls|
  +-------------------+------------------+------------------+------+------------------+-------------+-----------+
  |                  6|30.647222222222226|35.333333333333336|  32.0|136.83333333333334|          4.5|        1.5|
  +-------------------+------------------+------------------+------+------------------+-------------+-----------+

NOTE: You should create your spark scripts first before you do any of the Drake work.

*/
