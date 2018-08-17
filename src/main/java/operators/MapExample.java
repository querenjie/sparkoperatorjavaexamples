package operators;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Transformation
 * map
 * 语法（scala）：
 * def map[U: ClassTag](f: T => U): RDD[U]
 * 说明：
 * 将原来RDD的每个数据项通过map中的用户自定义函数f映射转变为一个新的元素
 */
public class MapExample implements Serializable {
    public void doit() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(MapExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> datas = Arrays.asList(
                "{'id':1,'name':'xl1','pwd':'xl123','sex':2}",
                "{'id':2,'name':'xl2','pwd':'xl123','sex':1}",
                "{'id':3,'name':'xl3','pwd':'xl123','sex':2}"
                );

        JavaRDD<String> datasRDD = sc.parallelize(datas);
        JavaRDD<User> mapRDD = datasRDD.map(new Function<String, User>() {
            public User call(String s) throws Exception {
                Gson gson = new Gson();
                return gson.fromJson(s, User.class);
            }
        });
        if (mapRDD != null) {
            mapRDD.foreach(new VoidFunction<User>() {
                public void call(User user) throws Exception {
                    System.out.println("id:" + user.id +
                    "\tname:" + user.name
                    + "\tpwd:" + user.pwd
                    + "\tsex:" + user.sex);
                }
            });
        }

        sc.close();
    }

    private class User {
        public Integer id;
        public String name;
        public String pwd;
        public Integer sex;
    }

    public static void main(String[] args) {
        MapExample mapExample = new MapExample();
        mapExample.doit();
    }
}
