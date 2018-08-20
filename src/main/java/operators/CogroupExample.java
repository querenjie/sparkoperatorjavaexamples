package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Transformation
 * cogroup
 * 语法（scala）：
 * JavaPairRDD<K,scala.Tuple2<Iterable<V>,Iterable<W>>> cogroup(JavaPairRDD<K,W> other,
 *                                                                Partitioner partitioner)
 *
 * JavaPairRDD<K,scala.Tuple3<Iterable<V>,Iterable<W1>,Iterable<W2>>> cogroup(JavaPairRDD<K,W1> other1,
 *                                                                                  JavaPairRDD<K,W2> other2,
 *                                                                                  Partitioner partitioner)
 *
 * JavaPairRDD<K,scala.Tuple4<Iterable<V>,Iterable<W1>,Iterable<W2>,Iterable<W3>>> cogroup(JavaPairRDD<K,W1> other1,
 *                                                                                                  JavaPairRDD<K,W2> other2,
 *                                                                                                  JavaPairRDD<K,W3> other3,
 *                                                                                                  Partitioner partitioner)
 *
 * JavaPairRDD<K,scala.Tuple2<Iterable<V>,Iterable<W>>> cogroup(JavaPairRDD<K,W> other)
 *
 * JavaPairRDD<K,scala.Tuple3<Iterable<V>,Iterable<W1>,Iterable<W2>>> cogroup(JavaPairRDD<K,W1> other1,
 *                                                                                  JavaPairRDD<K,W2> other2)
 *
 * JavaPairRDD<K,scala.Tuple4<Iterable<V>,Iterable<W1>,Iterable<W2>,Iterable<W3>>> cogroup(JavaPairRDD<K,W1> other1,
 *                                                                                                  JavaPairRDD<K,W2> other2,
 *                                                                                                  JavaPairRDD<K,W3> other3)
 *
 * JavaPairRDD<K,scala.Tuple2<Iterable<V>,Iterable<W>>> cogroup(JavaPairRDD<K,W> other,
 *                                                                int numPartitions)
 *
 * JavaPairRDD<K,scala.Tuple3<Iterable<V>,Iterable<W1>,Iterable<W2>>> cogroup(JavaPairRDD<K,W1> other1,
 *                                                                                  JavaPairRDD<K,W2> other2,
 *                                                                                  int numPartitions)
 *
 * JavaPairRDD<K,scala.Tuple4<Iterable<V>,Iterable<W1>,Iterable<W2>,Iterable<W3>>> cogroup(JavaPairRDD<K,W1> other1,
 *                                                                                                  JavaPairRDD<K,W2> other2,
 *                                                                                                  JavaPairRDD<K,W3> other3,
 *                                                                                                  int numPartitions)
 * 说明：
 * cogroup:对多个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
 */
public class CogroupExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        List<Tuple2<Integer, String>> datas1 = new ArrayList<Tuple2<Integer, String>>();
        datas1.add(new Tuple2<Integer, String>(1, "苹果"));
        datas1.add(new Tuple2<Integer, String>(2, "梨"));
        datas1.add(new Tuple2<Integer, String>(3, "香蕉"));
        datas1.add(new Tuple2<Integer, String>(4, "石榴"));

        List<Tuple2<Integer, Integer>> datas2 = new ArrayList<Tuple2<Integer, Integer>>();
        datas2.add(new Tuple2<Integer, Integer>(1, 7));
        datas2.add(new Tuple2<Integer, Integer>(2, 3));
        datas2.add(new Tuple2<Integer, Integer>(3, 8));
        datas2.add(new Tuple2<Integer, Integer>(4, 3));

        List<Tuple2<Integer, String>> datas3 = new ArrayList<Tuple2<Integer, String>>();
        datas3.add(new Tuple2<Integer, String>(1, "7"));
        datas3.add(new Tuple2<Integer, String>(2, "3"));
        datas3.add(new Tuple2<Integer, String>(3, "8"));
        datas3.add(new Tuple2<Integer, String>(4, "3"));
        datas3.add(new Tuple2<Integer, String>(4, "4"));
        datas3.add(new Tuple2<Integer, String>(4, "5"));
        datas3.add(new Tuple2<Integer, String>(4, "6"));

        JavaPairRDD<Integer, String> datas1RDD = javaSparkContext.parallelizePairs(datas1);
        JavaPairRDD<Integer, Integer> datas2RDD = javaSparkContext.parallelizePairs(datas2);
        JavaPairRDD<Integer, String> datas3RDD = javaSparkContext.parallelizePairs(datas3);

        System.out.println("datas2RDD.cogroup(datas3RDD),结果如下：");
        datas2RDD.cogroup(datas3RDD).foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<String>>>>() {
            public void call(Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<String>>> v) throws Exception {
                System.out.println(v);
            }
        });
        System.out.println("datas1RDD.cogroup(datas2RDD.cogroup(datas3RDD)),结果如下：");
        datas1RDD.cogroup(datas2RDD.cogroup(datas3RDD)).foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Tuple2<Iterable<Integer>, Iterable<String>>>>>>() {
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Tuple2<Iterable<Integer>, Iterable<String>>>>> v) throws Exception {
                System.out.println(v);
            }
        });
        System.out.println("根据主键排序过的结果：");
        datas1RDD.cogroup(datas2RDD, datas3RDD).sortByKey(true).foreach(new VoidFunction<Tuple2<Integer, Tuple3<Iterable<String>, Iterable<Integer>, Iterable<String>>>>() {
            public void call(Tuple2<Integer, Tuple3<Iterable<String>, Iterable<Integer>, Iterable<String>>> v) throws Exception {
                System.out.println(v);
            }
        });
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(CogroupExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        CogroupExample cogroupExample = new CogroupExample();
        cogroupExample.doit(javaSparkContext);

        javaSparkContext.close();
    }
}
