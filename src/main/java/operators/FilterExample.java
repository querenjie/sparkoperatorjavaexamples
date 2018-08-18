package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Transformation
 * filter
 * 语法（scala）：
 * def filter(f: T => Boolean): RDD[T]
 * 说明：
 * 对元素进行过滤，对每个元素应用f函数，返回值为true的元素在RDD中保留，返回为false的将过滤掉
 */
public class FilterExample implements Serializable {
    private void doit() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(FilterExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> datas = Arrays.asList(1,2,3,7,4,5,8);
        JavaRDD<Integer> datasRDD = sc.parallelize(datas);

        JavaRDD<Integer> filtedRDD = datasRDD.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer value) throws Exception {
                return value >= 3;
            }
        });

        filtedRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer value) throws Exception {
                System.out.println(value);
            }
        });

    }

    public static void main(String[] args) {
        FilterExample filterExample = new FilterExample();
        filterExample.doit();
    }
}
