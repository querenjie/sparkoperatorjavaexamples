package varibles;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Accumulator变量的应用。在Driver中获取累计值
 */
public class AccumulatorExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        List<Integer> datas = Arrays.asList(1,2,3,4,5);
        final Accumulator<Integer> sum = javaSparkContext.accumulator(0, "Accumulator Example");
        JavaRDD<Integer> datasRDD = javaSparkContext.parallelize(datas);
        datasRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer v) throws Exception {
                sum.add(v);
            }
        });
        System.out.println("sum = " + sum.value());
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(AccumulatorExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        AccumulatorExample accumulatorExample = new AccumulatorExample();
        accumulatorExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
