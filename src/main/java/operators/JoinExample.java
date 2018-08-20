package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Transformation
 * join
 * 语法（scala）：
 * JavaPairRDD<K,scala.Tuple2<V,W>> join(JavaPairRDD<K,W> other)
 * JavaPairRDD<K,scala.Tuple2<V,W>> join(
 *          JavaPairRDD<K,W> other,
 *          int numPartitions)
 * JavaPairRDD<K,scala.Tuple2<V,W>> join(
 *          JavaPairRDD<K,W> other,
 *          Partitioner partitioner)
 * 说明：
 * 对<K, V>和<K, W>进行join操作，返回(K, (V, W))外连接函数为leftOuterJoin、rightOuterJoin和fullOuterJoin
 */
public class JoinExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        List<Tuple2<Integer, String>> products = new ArrayList<Tuple2<Integer, String>>();
        products.add(new Tuple2<Integer, String>(1, "苹果"));
        products.add(new Tuple2<Integer, String>(2, "梨"));
        products.add(new Tuple2<Integer, String>(3, "香蕉"));
        products.add(new Tuple2<Integer, String>(4, "石榴"));

        List<Tuple2<Integer, Integer>> counts = new ArrayList<Tuple2<Integer, Integer>>();
        counts.add(new Tuple2<Integer, Integer>(1,7));
        counts.add(new Tuple2<Integer, Integer>(2,3));
        counts.add(new Tuple2<Integer, Integer>(3,8));
        counts.add(new Tuple2<Integer, Integer>(4,3));
        counts.add(new Tuple2<Integer, Integer>(5,9));

        JavaPairRDD<Integer, String> productsRDD = javaSparkContext.parallelizePairs(products);
        JavaPairRDD<Integer, Integer> countsRDD = javaSparkContext.parallelizePairs(counts);

        productsRDD.join(countsRDD).sortByKey(true).foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            public void call(Tuple2<Integer, Tuple2<String, Integer>> v) throws Exception {
                System.out.println(v);
            }
        });
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(JoinExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JoinExample joinExample = new JoinExample();
        joinExample.doit(javaSparkContext);

        javaSparkContext.close();
    }
}
