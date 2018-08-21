package order;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * 二次排序的例子
 * KeyWrapper是两个Key都按照从小到大的顺序排序
 * KeyWrapper2是两个Key都按照从大到小的顺序排序
 * KeyWrapper3是firstKey按照从大到小排序，sencondKey是按照从小到大排序
 * KeyWrapper4是firstKey按照从小到大排序，sencondKey是按照从大到小排序
 */
public class SecondarySortExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        JavaRDD<String> textRDD = javaSparkContext.textFile("sort.txt");
        textRDD.mapToPair(new PairFunction<String, KeyWrapper4, String>() {
            public Tuple2<KeyWrapper4, String> call(String row) throws Exception {
                String[] strArray = row.split(",");
                int firstKey = Integer.parseInt(strArray[0].trim());
                int secondKey = Integer.parseInt(strArray[2].trim());
                KeyWrapper4 keyWrapper = new KeyWrapper4(firstKey, secondKey);
                return new Tuple2<KeyWrapper4, String>(keyWrapper, row);
            }
        }).sortByKey().map(new Function<Tuple2<KeyWrapper4,String>, String>() {
            public String call(Tuple2<KeyWrapper4, String> tuple) throws Exception {
                return tuple._2;
            }
        }).foreach(new VoidFunction<String>() {
            public void call(String line) throws Exception {
                System.out.println(line);
            }
        });
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(SecondarySortExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        SecondarySortExample secondarySortExample = new SecondarySortExample();
        secondarySortExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
