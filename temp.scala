// Import the necessary classes
import org.apache.spark.sql.SparkSession

// Create a SparkSession
val spark = SparkSession.builder()
  .appName("Spark Shell")
  .master("local[*]")  // Set the master URL
  .getOrCreate()

// Create a SparkContext from the SparkSession
val sc = spark.sparkContext

// Now you can use the SparkContext
val data = Array(1, 2, 4, 5)
val distData = sc.parallelize(data)

