
import org.apache.spark.sql.SparkSession

object Book {

  def main(args: Array[String]) {

    val inputFile = args(0);

    val spark = SparkSession.builder
      .appName("spark-book-analysis")
      .master("local[*]")
      .getOrCreate();

    //1. Прочитать csv файл: book.csv
    val books = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(inputFile)

    books.show(100);

    //2. Вывести схему для dataframe
    books.printSchema();

    //3. Вывести количество записей
    println(books.count());

    //4.Вывести информацию по книгам у которых рейтинг выше 4.50
    books
      .select("*")
      .where("average_rating >= 4.50")
      .show(100);

    //5. Вывести средний рейтинг для всех книг.
    books.createOrReplaceTempView("books");
    spark
      .sql("select avg(average_rating) from books;")
      .show(1);

    //6. Вывести агрегированную инфорацию по количеству книг в диапазонах
    spark
      .sql(
        "select bookID," +
          " case when average_rating >= 0 and average_rating <= 1 then 1" +
          "  when average_rating > 1.0 and average_rating <= 2.0 then 2" +
          "  when average_rating > 2.0 and average_rating <= 3.0 then 3" +
          "  when average_rating > 3.0 and average_rating <= 4.0 then 4" +
          "  when average_rating > 4.0 and average_rating <= 5.0 then 5" +
          " else 0" +
          " end as rating_type" +
          " from books;").createOrReplaceTempView("books_ratings");

    spark.sql(
      "select rating_type, count(*) as count" +
        " from books_ratings" +
        " group by rating_type" +
        " order by rating_type;")
      .show(10);

    spark.stop();
  }
}