package operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Transformation
 * aggregateByKey
 * 举例：
 * 通过scala集合以并行化方式创建一个RDD
 * scala> val pairRdd = sc.parallelize(List(("cat",2),("cat",5),("mouse",4),("cat",12),("dog",12),("mouse",2)),2)
 * pairRdd 这个RDD有两个区，一个区中存放的是：
 * ("cat",2),("cat",5),("mouse",4)
 * 另一个分区中存放的是：
 * ("cat",12),("dog",12),("mouse",2)
 * 然后，执行下面的语句
 * scala > pairRdd.aggregateByKey(100)(math.max(_ , _),  _ + _ ).collect
 * 结果：
 * res0: Array[(String,Int)] = Array((dog,100),(cat,200),(mouse,200))
 * 下面是以上语句执行的原理详解：
 * aggregateByKey的意思是：按照key进行聚合
 * 第一步：将每个分区内key相同数据放到一起
 * 分区一
 * ("cat",(2,5)),("mouse",4)
 * 分区二
 * ("cat",12),("dog",12),("mouse",2)
 * 第二步：局部求最大值
 * 对每个分区应用传入的第一个函数，math.max(_ , _)，这个函数的功能是求每个分区中每个key的最大值
 * 这个时候要特别注意，aggregateByKe(100)(math.max(_ , _),_+_)里面的那个100，其实是个初始值
 * 在分区一中求最大值的时候,100会被加到每个key的值中，这个时候每个分区就会变成下面的样子
 * 分区一
 * ("cat",(2,5，100)),("mouse",(4，100))
 * 然后求最大值后变成：
 * ("cat",100), ("mouse",100)
 * 分区二
 * ("cat",(12,100)),("dog",(12.100)),("mouse",(2,100))
 * 求最大值后变成：
 * ("cat",100),("dog",100),("mouse",100)
 * 第三步：整体聚合
 * 将上一步的结果进一步的合成，这个时候100不会再参与进来
 * 最后结果就是：
 * (dog,100),(cat,200),(mouse,200)
 */
public class AggregateByKeyExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        List<Tuple2<Integer, Integer>> datas = new ArrayList<Tuple2<Integer, Integer>>();
        datas.add(new Tuple2<Integer, Integer>(1,3));
        datas.add(new Tuple2<Integer, Integer>(1,2));
        datas.add(new Tuple2<Integer, Integer>(2,4));
        datas.add(new Tuple2<Integer, Integer>(2,3));
        datas.add(new Tuple2<Integer, Integer>(1,5));

        javaSparkContext.parallelizePairs(datas, 2).aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("seq: " + v1 + "\t" + v2);
                return Math.max(v1, v2);
            }
        }, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("comb: " + v1 + "\t" + v2);
                return v1 + v2;
            }
        }).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            public void call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                System.out.println(integerIntegerTuple2);
            }
        });
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(AggregateByKeyExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        AggregateByKeyExample aggregateByKeyExample = new AggregateByKeyExample();
        aggregateByKeyExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
