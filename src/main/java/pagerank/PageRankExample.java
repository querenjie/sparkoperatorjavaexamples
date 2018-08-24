package pagerank;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * PageRank的例子，计算每个元素的PageRank值
 */
public class PageRankExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        int iter = 10;
        javaSparkContext.setCheckpointDir(".");
        JavaRDD<String> linesRDD = javaSparkContext.textFile("page.txt");

        JavaPairRDD<String, Iterable<String>> pairUrlRDD = linesRDD.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String line) throws Exception {
                String[] arr = line.split("\\s+");
                return new Tuple2<String, String>(arr[0], arr[1]);
            }
        }).distinct().groupByKey().cache();
        JavaPairRDD<String, Double> pairRankRDD = pairUrlRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Double>() {
            public Tuple2<String, Double> call(Tuple2<String, Iterable<String>> tup) throws Exception {
                return new Tuple2<String, Double>(tup._1, Double.valueOf(1.0));
            }
        });

        for (int i = 0; i < iter; i++) {
            JavaPairRDD<String, Double> pairContributesRDD = pairUrlRDD.join(pairRankRDD).values().flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
                public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> tup) throws Exception {
                    Iterator<String> urls = tup._1.iterator();
                    int size = 0;
                    List<String> urlList = new ArrayList<String>();
                    if (urls != null) {
                        while (urls.hasNext()) {
                            size++;
                            urlList.add(urls.next());
                        }
                    }

                    List<Tuple2<String, Double>> resultList = new ArrayList<Tuple2<String, Double>>();
                    if (size > 0) {
                        for (String url : urlList) {
                            double rank = tup._2 / size;
                            resultList.add(new Tuple2<String, Double>(url, rank));
                        }
                    }
                    return resultList.iterator();
                }
            });
            pairRankRDD = pairContributesRDD.reduceByKey(new Function2<Double, Double, Double>() {
                public Double call(Double v1, Double v2) throws Exception {
                    return v1 + v2;
                }
            }).mapToPair(new PairFunction<Tuple2<String,Double>, String, Double>() {
                public Tuple2<String, Double> call(Tuple2<String, Double> tup) throws Exception {
                    double newRank = 0.15 + 0.85 * tup._2;
                    return new Tuple2<String, Double>(tup._1, newRank);
                }
            });
            pairRankRDD.checkpoint();
        }

        pairRankRDD.mapToPair(new PairFunction<Tuple2<String,Double>, Double, String>() {
            public Tuple2<Double, String> call(Tuple2<String, Double> tup) throws Exception {
                return new Tuple2<Double, String>(tup._2, tup._1);
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Double,String>, String, Double>() {
            public Tuple2<String, Double> call(Tuple2<Double, String> tup) throws Exception {
                return new Tuple2<String, Double>(tup._2, tup._1);
            }
        }).foreach(new VoidFunction<Tuple2<String, Double>>() {
            public void call(Tuple2<String, Double> tup) throws Exception {
                System.out.println(tup._1 + "\t" + tup._2);
            }
        });
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(PageRankExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        PageRankExample pageRankExample = new PageRankExample();
        pageRankExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
