package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Transformation
 * groupByKey
 * 语法（scala）：
 * def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])]
 * def groupBy[K](      f: T => K,      numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])]
 * def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)      : RDD[(K, Iterable[T])]
 * # PairRDDFunctions类中
 * def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]
 * def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]
 * def groupByKey(): RDD[(K, Iterable[V])]
 * 说明：
 * 对<key, value>结构的RDD进行类似RMDB的group by聚合操作，具有相同key的RDD成员的value会被聚合在一起，返回的RDD的结构是(key, Iterator<value>)
 */
public class GroupByKeyExample implements Serializable {
    private void doit() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(GroupByKeyExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> datas1 = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        sc.parallelize(datas1).groupBy(new Function<Integer, Object>() {
            public Object call(Integer v) throws Exception {
                return (v % 2 == 0) ? "偶数" : "奇数";
            }
        }).foreach(new VoidFunction<Tuple2<Object, Iterable<Integer>>>() {
            public void call(Tuple2<Object, Iterable<Integer>> objectIterableTuple2) throws Exception {
                String key = (String)objectIterableTuple2._1();
                Iterable<Integer> iter = objectIterableTuple2._2();
                System.out.println(key + "\t" + iter);
            }
        });

        sc.close();
    }

    private void doit2() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(GroupByKeyExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> datas2 = Arrays.asList("dog", "tiger", "lion", "cat", "spider", "eagle");
        sc.parallelize(datas2).keyBy(new Function<String, Object>() {
            public Object call(String v) throws Exception {
                return v.length();
            }
        }).groupByKey().foreach(new VoidFunction<Tuple2<Object, Iterable<String>>>() {
            public void call(Tuple2<Object, Iterable<String>> objectIterableTuple2) throws Exception {
                System.out.println(objectIterableTuple2._1 + "\t" + objectIterableTuple2._2);
            }
        });

        sc.close();
    }

    public static void main(String[] args) {
        GroupByKeyExample groupByKeyExample = new GroupByKeyExample();
        groupByKeyExample.doit();
        System.out.println("---------------------------------------------------------------------");
        System.out.println("以下是doit2()的结果：");
        groupByKeyExample.doit2();
    }
}
