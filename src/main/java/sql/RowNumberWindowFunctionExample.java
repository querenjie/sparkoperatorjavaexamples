package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;

/**
 * 开窗函数的例子。
 * 开窗函数就是分组取TopN
 */
public class RowNumberWindowFunctionExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        HiveContext hiveContext = new HiveContext(javaSparkContext.sc());
        hiveContext.sql("drop table if exists sales");
        hiveContext.sql("create table if not exists sales (product string, category string, revenue bigint) row format delimited fields terminated by ','");
        hiveContext.sql("load data local inpath '/usr/local/tmp/sales.txt' into table sales");
        Dataset<Row> top2SaleDataset = hiveContext.sql(
                "select product, category, revenue from (" +
                        "select product, category, revenue," +
                        "row_number() over (partition by category order by revenue desc) rank " +
                        "from sales) tmp_sales " +
                        "where rank < 3");
        hiveContext.sql("drop table if exists top2_sales");
        top2SaleDataset.write().saveAsTable("top2_sales");
        top2SaleDataset.show();
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(RowNumberWindowFunctionExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        RowNumberWindowFunctionExample rowNumberWindowFunctionExample = new RowNumberWindowFunctionExample();
        rowNumberWindowFunctionExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
