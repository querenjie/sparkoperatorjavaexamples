package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Transformation
 * union
 * 语法（scala）：
 * def union(other: RDD[T]): RDD[T]
 *
 * 说明：
 * 合并两个RDD，不去重，要求两个RDD中的元素类型一致
 */
public class UnionExample implements Serializable {
    private void doit() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(UnionExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> datas1 = Arrays.asList("张三", "李四");
        List<String> datas2 = Arrays.asList("tom", "gim");

        JavaRDD<String> datas1RDD = sc.parallelize(datas1);
        JavaRDD<String> datas2RDD = sc.parallelize(datas2);

        JavaRDD<String> unionRDD = datas1RDD.union(datas2RDD);
        unionRDD.foreach(new VoidFunction<String>() {
            public void call(String v) throws Exception {
                System.out.println(v);
            }
        });

        sc.close();
    }

    public static void main(String[] args) {
        UnionExample unionExample = new UnionExample();
        unionExample.doit();
    }
}
