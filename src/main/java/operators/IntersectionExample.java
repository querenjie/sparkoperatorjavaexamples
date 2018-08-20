package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Transformation
 * intersection
 * 语法（scala）：
 * def intersection(other: RDD[T]): RDD[T]
 * 说明：
 * 返回两个RDD的交集
 */
public class IntersectionExample implements Serializable {
    private void doit() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(IntersectionExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> datas1 = Arrays.asList("张三", "李四", "tom");
        List<String> datas2 = Arrays.asList("tom", "gim");

        sc.parallelize(datas1).intersection(sc.parallelize(datas2)).foreach(new VoidFunction<String>() {
            public void call(String v) throws Exception {
                System.out.println(v);
            }
        });

        sc.close();
    }

    public static void main(String[] args) {
        IntersectionExample intersectionExample = new IntersectionExample();
        intersectionExample.doit();
    }
}
