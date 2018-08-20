package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Transformation
 * cartesian
 * 语法（scala）：
 * static <U> JavaPairRDD<T,U> cartesian(JavaRDDLike<U,?> other)
 * 说明：
 * 两个RDD进行笛卡尔积合并
 */
public class CartesianExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        List<String> names = Arrays.asList("张三", "李四", "王五");
        List<Integer> scores = Arrays.asList(60, 70, 80);

        JavaRDD<String> namesRDD = javaSparkContext.parallelize(names);
        JavaRDD<Integer> scoreRDD = javaSparkContext.parallelize(scores);

        JavaPairRDD<String, Integer> cartesianRDD = namesRDD.cartesian(scoreRDD);
        cartesianRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + "\t" + t._2());
            }
        });

    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(CartesianExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        CartesianExample cartesianExample = new CartesianExample();
        cartesianExample.doit(javaSparkContext);

        javaSparkContext.close();
    }
}
