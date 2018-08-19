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
 * sample
 * 语法（scala）：
 * def sample(
 *       withReplacement: Boolean,
 *       fraction: Double,
 *       seed: Long = Utils.random.nextLong): RDD[T]
 *
 * 说明：
 * 对RDD进行抽样，其中参数withReplacement为true时表示抽样之后还放回，可以被多次抽样，false表示不放回；fraction表示抽样比例；seed为随机数种子，比如当前时间戳
 */
public class SampleExample implements Serializable {
    private void doit() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(SampleExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> datas = Arrays.asList(1, 2, 3, 7, 4, 5, 8);
        if (datas == null || datas.size() == 0) {
            System.out.println("没有数据。。。。");
            return;
        }

        JavaRDD<Integer> datasRDD = sc.parallelize(datas);
        JavaRDD<Integer> sampleRDD = datasRDD.sample(false, 0.5, System.currentTimeMillis());
        sampleRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer v) throws Exception {
                System.out.println(v);
            }
        });

        sc.close();
    }

    public static void main(String[] args) {
        SampleExample sampleExample = new SampleExample();
        sampleExample.doit();
    }
}
