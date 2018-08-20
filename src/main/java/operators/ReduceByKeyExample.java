package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Transformation
 * reduceByKey
 * 语法（scala）：
 * def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)]
 * def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]
 * def reduceByKey(func: (V, V) => V): RDD[(K, V)]
 * 说明：
 * 对<key, value>结构的RDD进行聚合，对具有相同key的value调用func来进行reduce操作，func的类型必须是(V, V) => V
 */
public class ReduceByKeyExample implements Serializable {
    private void doit(JavaSparkContext sc) {
        try {
            JavaRDD<String> lines = sc.textFile("hdfs://192.168.1.20:9000/test/wordcount/in/inputFile1.txt");
            JavaRDD<String> wordsRDD = lines.flatMap(new FlatMapFunction<String, String>() {
                public Iterator<String> call(String line) throws Exception {
                    return Arrays.asList(line.split(" ")).iterator();
                }
            });
            JavaPairRDD<String, Integer> wordsCountTempRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
                public Tuple2<String, Integer> call(String v) throws Exception {
                    return new Tuple2<String, Integer>(v, 1);
                }
            });
            JavaPairRDD<String, Integer> resultRDD = wordsCountTempRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });
            resultRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                public void call(Tuple2<String, Integer> tuple2) throws Exception {
                    System.out.println(tuple2);
                }
            });
        } catch (Exception e) {
            System.out.println("出现错误，错误原因：" + e.toString());
        }

    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(ReduceByKeyExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        ReduceByKeyExample reduceByKeyExample = new ReduceByKeyExample();
        reduceByKeyExample.doit(sc);
        sc.close();
    }
}
