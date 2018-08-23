package varibles;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * 广播变量的使用
 */
public class BroadCastValueExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        int f = 2;
        List<Integer> datas = Arrays.asList(1,2,3,4,5);

        final Broadcast<Integer> broadcastF = javaSparkContext.broadcast(f);

        JavaRDD<Integer> datasRDD = javaSparkContext.parallelize(datas);
        JavaRDD<Integer> resultRDD = datasRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer v) throws Exception {
                return v * broadcastF.value();
            }
        });
        resultRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer v) throws Exception {
                System.out.println(v);
            }
        });
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(BroadCastValueExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        BroadCastValueExample broadCastValueExample = new BroadCastValueExample();
        broadCastValueExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
