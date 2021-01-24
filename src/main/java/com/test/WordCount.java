package com.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WordCount {

    private static void wordCount(String fileName) {

//        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");

        SparkSession spark = SparkSession.builder()
                .appName("spark-demo")
                .config("spark.master", "local")
                .getOrCreate();

//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

//        JavaRDD<String> inputFile = sparkContext.textFile(fileName);

        Dataset<Row> dataset = spark.read().text(fileName);

        dataset.show(false);


//        JavaRDD<String> wordsFromFile = inputFile.flatMap(row -> Arrays.asList(row.split(" ")));

//        JavaPairRDD countData = inputFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

//        System.out.println("Processing completed");
//        countData.saveAsTextFile("CountData");
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        wordCount(args[0]);
    }
}