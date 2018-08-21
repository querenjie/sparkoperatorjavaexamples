package operators.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Action
 * countByKey
 * 语法（java）：
 * java.util.Map<K,Long> countByKey()
 * 说明：
 * 仅适用于(K, V)类型，对key计数，返回(K, Int)
 */
public class CountByKeyExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        List<Tuple2<String, Integer>> datas = new ArrayList<Tuple2<String, Integer>>();
        datas.add(new Tuple2<String, Integer>("A", 1));
        datas.add(new Tuple2<String, Integer>("B", 6));
        datas.add(new Tuple2<String, Integer>("A", 2));
        datas.add(new Tuple2<String, Integer>("C", 1));
        datas.add(new Tuple2<String, Integer>("A", 7));
        datas.add(new Tuple2<String, Integer>("A", 8));

        Map<String, Long> result = javaSparkContext.parallelizePairs(datas).countByKey();
        System.out.println(result);
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(CountByKeyExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        CountByKeyExample countByKeyExample = new CountByKeyExample();
        countByKeyExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
