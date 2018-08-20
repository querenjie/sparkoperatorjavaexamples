package operators;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Transformation
 * repartitionAndSortWithinPartitions
 * 语法（scala）：
 * JavaPairRDD<K,V> repartitionAndSortWithinPartitions(Partitioner partitioner)
 *
 * JavaPairRDD<K,V> repartitionAndSortWithinPartitions(Partitioner partitioner,
 *                                                   java.util.Comparator<K> comp)
 * 说明：
 * 根据给定的Partitioner重新分区，并且每个分区内根据comp实现排序。
 */
public class RepartitionAndSortWithinPartitionsExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        List<String> datas = new ArrayList<String>();
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 100; j++) {
                datas.add(String.format("product%02d, url%03d", random.nextInt(10), random.nextInt(100)));
            }
        }
        JavaRDD<String> datasRDD = javaSparkContext.parallelize(datas);
        JavaPairRDD<String, String> pairRDD = datasRDD.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String v) throws Exception {
                String[] arr = v.split(",");
                return new Tuple2<String, String>(arr[0], arr[1]);
            }
        });
        JavaPairRDD<String, String> partSortRDD = pairRDD.repartitionAndSortWithinPartitions(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                return Integer.valueOf(((String)key).substring(7));
            }

            @Override
            public int numPartitions() {
                return 10;
            }
        });
        partSortRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> v) throws Exception {
                System.out.println(v);
            }
        });
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(RepartitionAndSortWithinPartitionsExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        RepartitionAndSortWithinPartitionsExample repartitionAndSortWithinPartitionsExample = new RepartitionAndSortWithinPartitionsExample();
        repartitionAndSortWithinPartitionsExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
