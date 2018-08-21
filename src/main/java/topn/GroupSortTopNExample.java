package topn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 读取文件，分组排序，提取每个分组的前N条数据
 */
public class GroupSortTopNExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext, final int topN) {
        JavaRDD<String> textRDD = javaSparkContext.textFile("group_sort_topn.txt");
        JavaPairRDD<String, Integer> pairRDD = textRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] arrOfLine = line.split(" ");
                String key = arrOfLine[0];
                Integer score = Integer.valueOf(arrOfLine[1]);
                return new Tuple2<String, Integer>(key, score);
            }
        });
        JavaPairRDD<String, List<Integer>> groupAndSortedTopNRDD = pairRDD.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, List<Integer>>() {
            public Tuple2<String, List<Integer>> call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                String key = tuple._1;
                Iterator<Integer> it = tuple._2.iterator();
                Integer[] intarr = new Integer[topN];
                while (it.hasNext()) {
                    Integer score = it.next();
                    //从小到大排序。插入排序
                    for (int i = 0; i < topN; i++) {
                        if (intarr[i] == null) {
                            intarr[i] = score;
                            break;
                        } else {
                            if (score.intValue() < intarr[i].intValue()) {
                                for (int j = topN - 1; j > i; j--) {
                                    intarr[j] = intarr[j - 1];
                                }
                                intarr[i] = score;
                                break;
                            }
                        }
                    }
                }
                List<Integer> intList = Arrays.asList(intarr);
                return new Tuple2<String, List<Integer>>(key, intList);
            }
        });
        JavaRDD<List<String>> resultRDD = groupAndSortedTopNRDD.map(new Function<Tuple2<String, List<Integer>>, List<String>>() {
            public List<String> call(Tuple2<String, List<Integer>> stringListTuple2) throws Exception {
                List<String> resultList = new ArrayList<String>();
                List<Integer> scoreList = stringListTuple2._2;
                if (scoreList != null) {
                    for (Integer score : scoreList) {
                        if (score != null) {
                            resultList.add(stringListTuple2._1 + " " + score);
                        } else {
                            break;
                        }
                    }
                }
                return resultList;
            }
        });
        resultRDD.foreach(new VoidFunction<List<String>>() {
            public void call(List<String> list) throws Exception {
                if (list != null) {
                    for (String line : list) {
                        System.out.println(line);
                    }
                }
            }
        });
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(GroupSortTopNExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        GroupSortTopNExample groupSortTopNExample = new GroupSortTopNExample();
        groupSortTopNExample.doit(javaSparkContext, 5);
        javaSparkContext.close();
    }
}
