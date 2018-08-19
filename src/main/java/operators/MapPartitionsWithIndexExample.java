package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Transformation
 * mapPartitionsWithIndex
 * 语法（scala）：
 * def mapPartitionsWithIndex[U: ClassTag](
 *       f: (Int, Iterator[T]) => Iterator[U],
 *       preservesPartitioning: Boolean = false): RDD[U]
 *
 * 说明：
 * 与mapPartitions类似，但输入会多提供一个整数表示分区的编号，所以func的类型是(Int, Iterator<T>) => Iterator<R>，多了一个Int
 */
public class MapPartitionsWithIndexExample implements Serializable {
    private void doit() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(MapPartitionsExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names = Arrays.asList("张三1", "李四1", "王五1", "张三2", "李四2",
                "王五2", "张三3", "李四3", "王五3", "张三4");
        if (names == null || names.size() == 0) {
            System.out.println("没数据。。。");
            return;
        }
        JavaRDD<String> namesRDD = sc.parallelize(names, 3);
        JavaRDD<String> mapPartitionsWithIndexRDD = namesRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            private static final long serialVersionUID = 1L;
            public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
                List<String> list = new ArrayList<String>();
                while (v2.hasNext()) {
                    list.add("分区索引:" + v1 + "\t" + v2.next());
                }
                return list.iterator();
            }
        }, true);
        mapPartitionsWithIndexRDD.foreach(new VoidFunction<String>() {
            public void call(String v) throws Exception {
                System.out.println(v);
            }
        });

        sc.close();
    }

    public static void main(String[] args) {
        MapPartitionsWithIndexExample mapPartitionsWithIndexExample = new MapPartitionsWithIndexExample();
        mapPartitionsWithIndexExample.doit();
    }
}
