package com.klqf.bigdata

import org.apache.spark.sql.SparkSession


object Main {
  //注意位置，要放在Main里面和main外面
  case class Person(classname:Int , name: String, age: Int, sex: String, subject: String, score:Int)

  def main(args: Array[String]): Unit = {
    println("Hello world!")

    val spark=SparkSession
      .builder()
      .appName("SparkSQL example")
      .master("local[*]")
      .getOrCreate()

    //要放在getOrCreate后面
    import spark.implicits._

//    val df=spark.read.text("hdfs://bigdata01:8020/geektime/test.txt")
//    val df=spark.read.csv("hdfs://bigdata01:8020/geektime/titanic.csv");
//    df.show()

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile("hdfs://bigdata01:8020/geektime/test.txt")
      .map(_.split(" "))
      .map(attributes => Person(attributes(0).trim.toInt,
        attributes(1),
        attributes(2).trim.toInt,
        attributes(3),
        attributes(4),
        attributes(5).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")
    peopleDF.show()

    var ans = peopleDF.filter("age<20").count()
    System.out.println("一共有多少个小于20岁的人参加考试？"+ans)

    ans =peopleDF.filter("age==20").count()
    System.out.println("一共有多少个等于20岁的人参加考试？"+ans)
//
    ans = peopleDF.filter("age>20").count()
    System.out.println("一共有多少个大于20岁的人参加考试？" + ans)

    ans = peopleDF.filter("sex=\"男\"").count()
    System.out.println("一共有多个男生参加考试？" + ans)
    ans = peopleDF.filter("sex=\"女\"").count()
    System.out.println(" 一共有多少个女生参加考试？" + ans)
    var ans2 = peopleDF.sqlContext.sql("select avg(score) from  people where subject=\"chinese\"");
    System.out.println("语文科目的平均成绩是多少？" + ans2.first().get(0));

    var ans3 = peopleDF.sqlContext.sql("select avg(score) from people where name in ( select name from  people group by name having sum(score)>150 and age >=19) and name not in (select distinct name from people where subject=\"math\" and score < 70)");
    System.out.println("总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？" + ans3.first().get(0));

    /*
    9. 语文科目的平均成绩是多少？

    10. 数学科目的平均成绩是多少？

    11. 英语科目的平均成绩是多少？

    12. 每个人平均成绩是多少？

    13. 12班平均成绩是多少？

    14. 12班男生平均总成绩是多少？

    15. 12班女生平均总成绩是多少？

    16. 13班平均成绩是多少？

    17. 13班男生平均总成绩是多少？

    18. 13班女生平均总成绩是多少？

    19. 全校语文成绩最高分是多少？

    20. 12班语文成绩最低分是多少？

    21. 13班数学最高成绩是多少？

    22. 总成绩大于150分的12班的女生有几个？

    23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
     */


  }
}