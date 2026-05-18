import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Census")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

  AAAAA

    // 3.1 Đọc dữ liệu nha  OK Nhe
    val dfRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:/Kiến thức cơ sở ngành/Big Data/census_1000.csv")

    println("===== DATA =====")
    dfRaw.show()

    // Chuẩn hóa + ép kiểu
    val df = dfRaw
      .withColumn("age", $"age".cast("int"))
      .withColumn("education-num", $"`education-num`".cast("int"))
      .withColumn("gender", trim(lower($"gender")))
      .withColumn("marital-status", trim(lower($"marital-status")))
      .withColumn("ethnicity", trim(lower($"ethnicity")))
      .withColumn("education", trim(lower($"education")))

    // 3.2 Số nhóm tuổi
    println("===== 3.2 =====")
    val ageGroups = df.select("age").distinct()
    ageGroups.show()
    println("Số nhóm tuổi: " + ageGroups.count())

    // 3.3 Nhóm tuổi có Bachelors
    println("===== 3.3 =====")
    val bachelorAge = df.filter($"education" === "bachelors")
      .select("age")
      .distinct()

    bachelorAge.show()
    println("Số nhóm tuổi có Bachelors: " + bachelorAge.count())

    // 3.4 Schema
    println("===== 3.4 =====")
    df.printSchema()

    // 3.5 Nhóm tuổi có tổng education-num lớn nhất
    println("===== 3.5 =====")
    df.groupBy("age")
      .agg(sum("education-num").alias("total_edu"))
      .orderBy(desc("total_edu"))
      .show()

    // 3.6 Nữ + never-married + white
    println("===== 3.6 =====")
    val femaleGroup = df.filter(
      $"gender" === "female" &&
        $"marital-status" === "never-married" &&
        $"ethnicity" === "white"
    ).select("age")

    femaleGroup.show()
    println("Tổng số dòng thỏa điều kiện: " + femaleGroup.count())

    // 3.7 Nhóm tuổi là người da trắng
    println("===== 3.7 =====")
    val whiteAge = df.filter($"ethnicity" === "white")
      .select("age")
      .distinct()

    whiteAge.show()
    println("Số nhóm tuổi là người da trắng: " + whiteAge.count())

    // 3.8 marital-status bắt đầu bằng married
    println("===== 3.8 =====")
    val marriedAge = df.filter($"marital-status".startsWith("married"))
      .select("age")
      .distinct()

    marriedAge.show()
    println("Số nhóm tuổi married: " + marriedAge.count())

    // 3.9 Pivot
    println("===== 3.9 =====")
    df.groupBy("occupation")
      .pivot("gender")
      .sum("education-num")
      .show()

    // 3.10 Tạo cột mới
    println("===== 3.10 =====")
    val dfFinal = df.withColumn(
      "gender_short",
      when($"gender" === "male", "M")
        .when($"gender" === "female", "F")
        .otherwise("Unknown")
    )

    dfFinal.show()

    spark.stop()
  }
}