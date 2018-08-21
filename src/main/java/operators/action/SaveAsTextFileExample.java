package operators.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Action
 * saveAsTextFile
 * 语法（java）：
 * void saveAsTextFile(String path)
 *
 * void saveAsTextFile(String path,
 *                   Class<? extends org.apache.hadoop.io.compress.CompressionCodec> codec)
 * 说明：
 * 将RDD转换为文本内容并保存至路径path下，可能有多个文件(和partition数有关)。路径path可以是本地路径或HDFS地址，转换方法是对RDD成员调用toString函数
 */
public class SaveAsTextFileExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        List<Integer> datas = Arrays.asList(5,6,2,1,7,8);
        javaSparkContext.parallelize(datas,2).saveAsTextFile("d://test/spark_data/aaa");
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(SaveAsTextFileExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        SaveAsTextFileExample saveAsTextFileExample = new SaveAsTextFileExample();
        saveAsTextFileExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
