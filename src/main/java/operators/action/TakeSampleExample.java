package operators.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Action
 * takeSample
 * 语法（java）：
 * static java.util.List<T> takeSample(boolean withReplacement,
 *                          int num,
 *                          long seed)
 * 说明：
 * 和sample用法相同，只不第二个参数换成了个数。返回也不是RDD，而是collect。
 */
public class TakeSampleExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        List<Integer> datas = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        List<Integer> resultList = javaSparkContext.parallelize(datas).takeSample(false, 3, 1);
        if (resultList != null) {
            for (Integer i : resultList) {
                System.out.println(i);
            }
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(TakeSampleExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        TakeSampleExample takeSampleExample = new TakeSampleExample();
        takeSampleExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
