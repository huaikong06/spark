package com.transfar.main;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;


/**
 * @author wangyu
 * @date 2020/5/9 14:30
 * @description
 * java开发spark测试程序
 */
public class WordCountCluster {


    public static void main(String[] args) throws InterruptedException{

        System.out.println("调用集群spark开始");
        //创建配置参数
        SparkConf conf = new SparkConf().setAppName("WordCountOnLine");
                //.setMaster("spark://192.168.1.72:7077");
        //创建sparkcontext
        JavaSparkContext jsc= new JavaSparkContext(conf);

        //获取文件
        JavaRDD<String> lines=jsc.textFile("hdfs://192.168.1.72:9000/wytest/import/spark.txt");

        System.out.println("查询获取的spark文件:"+lines);

        System.out.println("开始map操作!");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }


        });

        System.out.println("开始reduce操作!");
        JavaPairRDD<String, Integer> pairs = words.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }

                });

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }

                });


        System.out.println("打印执行结果!");
        wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }

        });

        //计算结束
        jsc.close();





    }

}
