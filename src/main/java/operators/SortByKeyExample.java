package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Transformation
 * sortByKey
 * 语法（scala）：
 * JavaRDD<T> sortBy(Function<T,S> f,
 *                   boolean ascending,
 *                   int numPartitions)
 * JavaPairRDD<K,V> sortByKey()
 * JavaPairRDD<K,V> sortByKey(boolean ascending)
 * JavaPairRDD<K,V> sortByKey(boolean ascending,
 *                        int numPartitions)
 * JavaPairRDD<K,V> sortByKey(java.util.Comparator<K> comp)
 * JavaPairRDD<K,V> sortByKey(java.util.Comparator<K> comp,
 *                        boolean ascending)
 * JavaPairRDD<K,V> sortByKey(java.util.Comparator<K> comp,
 *                        boolean ascending,
 *                        int numPartitions)
 * 说明：
 * 对<key, value>结构的RDD进行升序或降序排列
 * 参数：
 * comp：排序时的比较运算方式。
 * ascending：false降序；true升序。
 */
public class SortByKeyExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        List<Integer> datas = Arrays.asList(60, 70, 80, 55, 45, 75);
        JavaRDD<Integer> datasRDD = javaSparkContext.parallelize(datas);
        JavaRDD<Integer> resultRDD =  datasRDD.sortBy(new Function<Integer, Integer>() {
            public Integer call(Integer v) throws Exception {
                return v;
            }
        }, false, 1);
        resultRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer v) throws Exception {
                System.out.println(v);
            }
        });
    }

    private void doit2(JavaSparkContext javaSparkContext) {
        List<Tuple2<Integer, Integer>> datas = new ArrayList<Tuple2<Integer, Integer>>();
        datas.add(new Tuple2<Integer, Integer>(3,3));
        datas.add(new Tuple2<Integer, Integer>(2,2));
        datas.add(new Tuple2<Integer, Integer>(1,4));
        datas.add(new Tuple2<Integer, Integer>(2,3));

        JavaPairRDD<Integer, Integer> datasRDD = javaSparkContext.parallelizePairs(datas);
        datasRDD.sortByKey(false).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            public void call(Tuple2<Integer, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(SortByKeyExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        SortByKeyExample sortByKeyExample = new SortByKeyExample();
        sortByKeyExample.doit(javaSparkContext);
        System.out.println("--------------------------------------------------");
        System.out.println("以下是调用doit2()的结果：");
        sortByKeyExample.doit2(javaSparkContext);
        javaSparkContext.close();
    }
}
