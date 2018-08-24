package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * UDF的例子。UDF的特点是传入多个数据就返回多个数据。
 */
public class UdfExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        List<String> nameList = Arrays.asList("name1", "name2", "name3", "name4");
        JavaRDD<String> nameRDD = javaSparkContext.parallelize(nameList, 4);
        JavaRDD<Row> nameRowRDD = nameRDD.map(new Function<String, Row>() {
            public Row call(String name) throws Exception {
                return RowFactory.create(name);
            }
        });
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> nameDataset = sqlContext.createDataFrame(nameRowRDD, schema);
        nameDataset.registerTempTable("t_name");

        /*****************以下定义UDF和使用UDF**********************/
        //这句话是定义和注册一个临时的UDF
        sqlContext.udf().register("length", new UDF1<String, Integer>() {
            public Integer call(String v) throws Exception {
                return v.length();
            }
        }, DataTypes.IntegerType);
        //然后使用UDF
        sqlContext.sql("select name, length(name) as length from t_name").show();
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(UdfExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        UdfExample udfExample = new UdfExample();
        udfExample.doit(javaSparkContext);
        javaSparkContext.close();
    }
}
