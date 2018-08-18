package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *  Transformation
 *  flatMap
 *  语法（scala）：
 *  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]
 *  说明：
 *  与map类似，但每个输入的RDD成员可以产生0或多个输出成员
 */
public class FlatMapExample implements Serializable {
    private void doit() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(FlatMapExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList(
                "aa,bb,cc",
                "cxf,spring,struts2",
                "java,C++,javaScript"
        );

        JavaRDD<String> dataRDD = sc.parallelize(data);
        JavaRDD<String> flatMappedRDD = dataRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String value) throws Exception {
                List<String> list = Arrays.asList(value.split(","));
                return list.iterator();
            }
        });

        if (flatMappedRDD != null) {
            flatMappedRDD.foreach(new VoidFunction<String>() {
                public void call(String value) throws Exception {
                    System.out.println(value);
                }
            });
        }

        sc.close();
    }

    public static void main(String[] args) {
        FlatMapExample flatMapExample = new FlatMapExample();
        flatMapExample.doit();
    }
}
