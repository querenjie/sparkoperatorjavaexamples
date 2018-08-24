package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * SparkSql jdbc mysql的例子
 */
public class SparkSqlJdbcMysqlExample implements Serializable {
    private void createInsertData(JavaSparkContext javaSparkContext) {
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        List<Tuple2<Integer, String>> dataList = new ArrayList<Tuple2<Integer, String>>();
        dataList.add(new Tuple2<Integer, String>(1, "name1"));
        dataList.add(new Tuple2<Integer, String>(2, "name2"));
        dataList.add(new Tuple2<Integer, String>(3, "name3"));
        dataList.add(new Tuple2<Integer, String>(4, "name4"));

        JavaRDD<Row> data = javaSparkContext.parallelizePairs(dataList).map(new Function<Tuple2<Integer,String>, Row>() {
            public Row call(Tuple2<Integer, String> tuple) throws Exception {
                Row row = RowFactory.create(tuple._1, tuple._2);
                return row;
            }
        });

        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> peopleDataset = sqlContext.createDataFrame(data, schema);
        peopleDataset.createOrReplaceTempView("people");
        Dataset<Row> resultDataset = sqlContext.sql("select id, name from people");
        resultDataset.show();

        /********************以下是向mysql中创建表和写入数据**************************/
        Properties prop = new Properties();
        prop.setProperty("user", "root");
        prop.setProperty("password", "root");
        String url = "jdbc:mysql://192.168.1.20:3306/test";
        peopleDataset.write().jdbc(url, "people", prop);
        System.out.println("请查看mysql中的people表。");
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(SparkSqlJdbcMysqlExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        SparkSqlJdbcMysqlExample sparkSqlJdbcMysqlExample = new SparkSqlJdbcMysqlExample();
        sparkSqlJdbcMysqlExample.createInsertData(javaSparkContext);

        javaSparkContext.close();
    }
}
