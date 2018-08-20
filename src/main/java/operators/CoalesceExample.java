package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Transformation
 * coalesce
 * 语法（scala）：
 * JavaRDD<T> coalesce(int numPartitions)
 *
 * JavaRDD<T> coalesce(int numPartitions,
 *                   boolean shuffle)
 *
 * JavaPairRDD<K,V> coalesce(int numPartitions)
 *
 * JavaPairRDD<K,V> coalesce(int numPartitions,
 *                         boolean shuffle)
 * 说明：
 * 用于将RDD进行重分区，使用HashPartitioner。且该RDD的分区个数等于numPartitions个数。如果shuffle设置为true，则会进行shuffle。
 */
public class CoalesceExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        List<String> datas = Arrays.asList("hi", "hello", "how", "are", "you");
        JavaRDD<String> datasRDD = javaSparkContext.parallelize(datas, 4);
        System.out.println("RDD的分区数: " + datasRDD.partitions().size());
        JavaRDD<String> datasRDD2 = datasRDD.coalesce(2);
        System.out.println("RDD的分区数: " + datasRDD2.partitions().size());
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(CoalesceExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        CoalesceExample coalesceExample = new CoalesceExample();
        coalesceExample.doit(javaSparkContext);

        javaSparkContext.close();
    }
}
