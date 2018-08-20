package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Transformation
 * distinct
 * 语法（scala）：
 * def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
 * def distinct(): RDD[T]
 * 说明：
 * 对原RDD进行去重操作，返回RDD中没有重复的成员
 */
public class DistinctExample implements Serializable {
    private void doit() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(DistinctExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> datas1 = Arrays.asList("张三", "李四", "tom", "张三");

        sc.parallelize(datas1).distinct().foreach(new VoidFunction<String>() {
            public void call(String v) throws Exception {
                System.out.println(v);
            }
        });

        sc.close();
    }

    public static void main(String[] args) {
        DistinctExample distinctExample = new DistinctExample();
        distinctExample.doit();
    }
}
