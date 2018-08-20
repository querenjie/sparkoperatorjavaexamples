package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Transformation
 * repartition
 * 语法（scala）：
 * JavaRDD<T> repartition(int numPartitions)
 *
 * JavaPairRDD<K,V> repartition(int numPartitions)
 * 说明：
 * 该函数其实就是coalesce函数第二个参数为true的实现
 */
public class RepartitionExample implements Serializable {
    private void  doit(JavaSparkContext javaSparkContext) {
        List<String> datas = Arrays.asList("hi", "hello", "how", "are", "you");
        JavaRDD<String> datasRDD = javaSparkContext.parallelize(datas, 2);
        System.out.println("分区数量：" + datasRDD.partitions().size());
        JavaRDD<String> datasRDD2 = datasRDD.repartition(4);
        System.out.println("分区数量：" + datasRDD2.partitions().size());
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(RepartitionExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        RepartitionExample repartitionExample = new RepartitionExample();
        repartitionExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
