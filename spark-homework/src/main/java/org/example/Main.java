package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        String logFile = "E:\\download\\test.txt"; // Should be some file on your system
        SparkSession spark = SparkSession.builder()
//                .master("local")
                .appName("Simple Application").getOrCreate();

//        Dataset<String> logData = spark.read().textFile(logFile).cache();
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile(logFile)
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(" ");
                    Person person = new Person();
                    ////    班级  姓名  年龄  性别  科目 成绩
                    ////12 宋江 25 男 chinese 50
                    person.setClassname(Integer.parseInt(parts[0].trim()));
                    person.setName(parts[1]);
                    person.setAge(Integer.parseInt(parts[2].trim()));
                    person.setSex(parts[3]);
                    person.setSubject(parts[4]);
                    person.setScore(Integer.parseInt(parts[5].trim()));
                    return person;
                });
        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

        // SQL statements can be run by using the sql methods provided by spark
//        2. 一共有多少个小于20岁的人参加考试？
        Dataset<Row> queryds = spark.sql("select count(*) from  people where age<20");
        int count = (int) queryds.first().getLong(0);
        System.out.println("一共有多少个小于20岁的人参加考试？"+count);
        queryds = spark.sql("select count(*) from  people where age=20");
        count = (int) queryds.first().getLong(0);
        System.out.println("一共有多少个等于20岁的人参加考试？"+count);
        queryds = spark.sql("select count(*) from  people where age>20");
        count = (int) queryds.first().getLong(0);
        System.out.println("一共有多少个大于20岁的人参加考试？"+count);
        queryds = spark.sql("select count(*) from  people where sex=\"男\"");
        count = (int) queryds.first().getLong(0);
        System.out.println("一共有多个男生参加考试？"+count);
        queryds = spark.sql("select count(*) from  people where sex=\"女\"");
        count = (int) queryds.first().getLong(0);
        System.out.println("一共有多少个女生参加考试？"+count);
        queryds = spark.sql("select count(*) from  people where classname=12");
        count = (int) queryds.first().getLong(0);
        System.out.println("12班有多少人参加考试？"+count);
        queryds = spark.sql("select count(*) from  people where classname=13");
        count = (int) queryds.first().getLong(0);
        System.out.println("13班有多少人参加考试？"+count);
        queryds = spark.sql("select avg(score) from  people where subject=\"chinese\"");
        double avg = queryds.first().getDouble(0);
        System.out.println("语文科目的平均成绩是多少？"+avg);
        queryds = spark.sql("select avg(score) from  people where subject=\"math\"");
        avg = queryds.first().getDouble(0);
        System.out.println("数学科目的平均成绩是多少？"+avg);
        queryds = spark.sql("select avg(score) from  people where subject=\"english\"");
        avg =  queryds.first().getDouble(0);
        System.out.println("英语科目的平均成绩是多少？"+avg);
        queryds = spark.sql("select avg(score) from  people");
        avg =  queryds.first().getDouble(0);
        System.out.println("每个人平均成绩是多少？"+avg);
        queryds = spark.sql("select avg(score) from  people where classname=12");
        avg = queryds.first().getDouble(0);
        System.out.println("12班平均成绩是多少？"+avg);
        queryds = spark.sql("select avg(score) from  people where classname=12 and sex=\"男\"");
        avg =  queryds.first().getDouble(0);
        System.out.println("12班男生平均总成绩是多少？"+avg);
        queryds = spark.sql("select avg(score) from  people where classname=12 and sex=\"女\"");
        avg =  queryds.first().getDouble(0);
        System.out.println("12班女生平均总成绩是多少？"+avg);
        queryds = spark.sql("select avg(score) from  people where classname=13");
        avg =  queryds.first().getDouble(0);
        System.out.println("13班平均成绩是多少？"+avg);
        queryds = spark.sql("select avg(score) from  people where classname=13 and sex=\"男\"");
        avg =  queryds.first().getDouble(0);
        System.out.println("13班男生平均总成绩是多少？"+avg);
        queryds = spark.sql("select avg(score) from  people where classname=13 and sex=\"女\"");
        avg =  queryds.first().getDouble(0);
        System.out.println("13班女生平均总成绩是多少？"+avg);
        queryds = spark.sql("select max(score) from  people where subject=\"chinese\"");
        count =  queryds.first().getInt(0);
        System.out.println("全校语文成绩最高分是多少？"+count);
        queryds = spark.sql("select min(score) from  people where classname=12 and subject=\"chinese\"");
        count = queryds.first().getInt(0);
        System.out.println("12班语文成绩最低分是多少？"+count);
        queryds = spark.sql("select max(score) from  people where classname=12 and subject=\"math\"");
        count =  queryds.first().getInt(0);
        System.out.println("13班数学最高成绩是多少？"+count);
        queryds = spark.sql("select count(*) from (select sum(score) as sumscore from  people where classname=12 and sex=\"女\" group by name having sumscore>150)");
        count = (int) queryds.first().getLong(0);
        System.out.println("总成绩大于150分的12班的女生有几个？"+count);
        queryds = spark.sql("select avg(score) from people where name in ( select name from  people group by name having sum(score)>150 and age >=19) and name not in (select distinct name from people where subject=\"math\" and score < 70)");
        avg = queryds.first().getDouble(0);
        System.out.println("总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？"+avg);
        spark.stop();
    }
}