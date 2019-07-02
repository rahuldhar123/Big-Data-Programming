import org.apache.spark._

object actions {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils" )
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("actions").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val nums = sc.parallelize(Array(1,2,3))

    println(nums.collect())

    println(nums.count())
    println(nums.saveAsTextFile("D:\\Drivers\\github\\Big-Data-Programming\\Module2\\ICP1\\file.txt"))
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    //val input = sc.textFile("input.txt")
    // Split up into words.
    //val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    //val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a text file, causing evaluation.
    //counts.saveAsTextFile("output")
  }
}
