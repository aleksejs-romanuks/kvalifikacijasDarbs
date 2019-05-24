import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.uberpalform.kafka.JsonRequestParser
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FunSuite


//documentation https://github.com/MrPowers/spark-fast-tests
class JsonRequestParserTest extends FunSuite with DatasetComparer  {

  val spark = SparkSession.builder
    .master("local")
    .appName("testing")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  test("Parsing good message"){
    val expectedData = Seq(Row("0000000002", "client", "new", "2019-01-05 13:31:15"))
    val expectedSchema = List(
      StructField("customerId", StringType, true),
      StructField("role", StringType, true),
      StructField("status", StringType, true),
      StructField("requestDateTime", StringType, true)
    )
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val source =  spark.sparkContext.parallelize(List("{ \"customerId\":\"0000000002\", \"role\":\"client\", \"status\":\"new\", \"requestDateTime\": \"2019-01-05 13:31:15\" }"))
    val jsonRequestParserObject = new JsonRequestParser(spark)
    val sourceDF = jsonRequestParserObject.parseJsonRDDtoDF(source)

    assertSmallDatasetEquality(sourceDF, expectedDF)
  }

  test("Parsing bad message"){
    val expectedSchema = List(
      StructField("customerId", StringType, true),
      StructField("role", StringType, true),
      StructField("status", StringType, true),
      StructField("requestDateTime", StringType, true)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      StructType(expectedSchema)
    )

    val source =  spark.sparkContext.parallelize(List("bad json message"))
    val jsonRequestParserObject = new JsonRequestParser(spark)
    val sourceDF = jsonRequestParserObject.parseJsonRDDtoDF(source)

    assertSmallDatasetEquality(sourceDF, expectedDF)
  }

  test("Parsing multiple messages"){
    val expectedData = Seq(
      Row("0000000001", "client", "new", "2019-01-05 13:31:15"),
      Row("0000000002", "client", "new", "2019-01-05 13:31:15"),
      Row("0000000003", "client", "new", "2019-01-05 13:31:15"),
      Row("0000000004", "client", "new", "2019-01-05 13:31:15")
    )
    val expectedSchema = List(
      StructField("customerId", StringType, true),
      StructField("role", StringType, true),
      StructField("status", StringType, true),
      StructField("requestDateTime", StringType, true)
    )
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val source =  spark.sparkContext.parallelize(List(
      "{ \"customerId\":\"0000000001\", \"role\":\"client\", \"status\":\"new\", \"requestDateTime\": \"2019-01-05 13:31:15\" }",
      "{ \"customerId\":\"0000000002\", \"role\":\"client\", \"status\":\"new\", \"requestDateTime\": \"2019-01-05 13:31:15\" }",
      "{ \"customerId\":\"0000000003\", \"role\":\"client\", \"status\":\"new\", \"requestDateTime\": \"2019-01-05 13:31:15\" }",
      "{ \"customerId\":\"0000000004\", \"role\":\"client\", \"status\":\"new\", \"requestDateTime\": \"2019-01-05 13:31:15\" }"
    ))
    val jsonRequestParserObject = new JsonRequestParser(spark)
    val sourceDF = jsonRequestParserObject.parseJsonRDDtoDF(source)

    assertSmallDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
  }

  test("Parsing multiple messages with one bad message"){
    val expectedData = Seq(
      Row("0000000001", "client", "new", "2019-01-05 13:31:15"),
      Row("0000000002", "client", "new", "2019-01-05 13:31:15"),
      Row("0000000003", "client", "new", "2019-01-05 13:31:15")
    )
    val expectedSchema = List(
      StructField("customerId", StringType, true),
      StructField("role", StringType, true),
      StructField("status", StringType, true),
      StructField("requestDateTime", StringType, true)
    )
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val source =  spark.sparkContext.parallelize(List(
      "{ \"customerId\":\"0000000001\", \"role\":\"client\", \"status\":\"new\", \"requestDateTime\": \"2019-01-05 13:31:15\" }",
      "{ \"customerId\":\"0000000002\", \"role\":\"client\", \"status\":\"new\", \"requestDateTime\": \"2019-01-05 13:31:15\" }",
      "bad json message",
      "{ \"customerId\":\"0000000003\", \"role\":\"client\", \"status\":\"new\", \"requestDateTime\": \"2019-01-05 13:31:15\" }"
    ))
    val jsonRequestParserObject = new JsonRequestParser(spark)
    val sourceDF = jsonRequestParserObject.parseJsonRDDtoDF(source)

    assertSmallDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
  }


}