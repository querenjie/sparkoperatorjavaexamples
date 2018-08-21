package operators.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Action
 * takeOrdered
 * 语法（java）：
 * java.util.List<T> takeOrdered(int num)
 *
 * java.util.List<T> takeOrdered(int num,
 *                           java.util.Comparator<T> comp)
 * 说明：
 * 用于从RDD中，按照默认（升序）或指定排序规则，返回前num个元素。
 */
public class TakeOrderedExample implements Serializable {
    public static final long serialVersionUID = 1L;
    private void doit(JavaSparkContext javaSparkContext) {
        List<Integer> datas = Arrays.asList(5,6,2,1,7,8);
        JavaRDD<Integer> datasRDD = javaSparkContext.parallelize(datas);
        List<Integer> orderedList = datasRDD.takeOrdered(3);
        if (orderedList != null) {
            for (Integer i : orderedList) {
                System.out.println(i);
            }
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(TakeOrderedExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        TakeOrderedExample takeOrderedExample = new TakeOrderedExample();
        takeOrderedExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
