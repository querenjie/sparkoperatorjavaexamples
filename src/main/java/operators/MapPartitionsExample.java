package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Transformation
 * mapPartitions
 * 语法（scala）：
 * def mapPartitions[U: ClassTag](
 *       f: Iterator[T] => Iterator[U],
 *       preservesPartitioning: Boolean = false): RDD[U]
 *
 * 说明：
 * 与Map类似，但map中的func作用的是RDD中的每个元素，而mapPartitions中的func作用的对象是RDD的一整个分区。
 * 所以func的类型是Iterator<T> => Iterator<U>，其中T是输入RDD元素的类型。
 * preservesPartitioning表示是否保留输入函数的partitioner，默认false。
 */
public class MapPartitionsExample implements Serializable {
    private void doit() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(MapPartitionsExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> datas = Arrays.asList("张三1", "李四1", "王五1", "张三2", "李四2",
                "王五2", "张三3", "李四3", "王五3", "张三4");
        JavaRDD<String> datasRDD = sc.parallelize(datas, 3);
        JavaRDD<String> mapPartitionsRDD = datasRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            int count = 0;
            public Iterator<String> call(Iterator<String> values) throws Exception {
                List<String> list = new ArrayList<String>();
                while (values.hasNext()) {
                    list.add("分区索引:" + count++ + "\t" + values.next());
                }
                return list.iterator();
            }
        });
        // 从集群获取数据到本地内存中
        //List<String> result = mapPartitionsRDD.collect();
        mapPartitionsRDD.foreach(new VoidFunction<String>() {
            public void call(String value) throws Exception {
                System.out.println(value);
            }
        });
        sc.close();
    }

    public static void main(String[] args) {
        MapPartitionsExample mapPartitionsExample = new MapPartitionsExample();
        mapPartitionsExample.doit();
    }
}
