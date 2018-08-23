package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * SparkSQL访问json格式的文件
 */
public class SparkSqlJsonExample implements Serializable {
    private void doit(JavaSparkContext javaSparkContext) {
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        Dataset<Row> studentScoreDataset = sqlContext.read().json("student_score.json");
        studentScoreDataset.registerTempTable("student_score");
        //查询出分数在80以上的学生姓名和成绩
        Dataset<Row> goodStudentsDataset = sqlContext.sql("select name, score from student_score where score >= 80");
        goodStudentsDataset.toJavaRDD().foreach(new VoidFunction<Row>() {
            public void call(Row row) throws Exception {
                String name = row.get(0).toString();
                Integer score = Integer.parseInt(row.get(1).toString());
                System.out.println("name:" + name + "\t" + "score:" + score);
            }
        });
    }

    private void doit2(JavaSparkContext javaSparkContext) {
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        List<String> studentInfoList = new ArrayList<String>();
        studentInfoList.add("{\"name\":\"stud_01\",\"age\":18}");
        studentInfoList.add("{\"name\":\"stud_02\",\"age\":17}");
        studentInfoList.add("{\"name\":\"stud_03\",\"age\":19}");
        JavaRDD<String> studentInfoListRDD = javaSparkContext.parallelize(studentInfoList);
        Dataset<Row> studentInfoDataset = sqlContext.read().json(studentInfoListRDD);
        studentInfoDataset.registerTempTable("student_infos");
        Dataset<Row> resultDataset = sqlContext.sql("select name, age from student_infos where age < 19");
        resultDataset.toJavaRDD().foreach(new VoidFunction<Row>() {
            public void call(Row row) throws Exception {
                String name = row.get(0).toString();
                Integer age = Integer.parseInt(row.get(1).toString());
                System.out.println("name:" + name + "\t" + "age:" + age);
            }
        });
    }

    private void doit3(JavaSparkContext javaSparkContext) {
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        Dataset<Row> studentScoreDataset = sqlContext.read().json("student_score.json");
        studentScoreDataset.registerTempTable("student_score");

        List<String> studentInfoList = new ArrayList<String>();
        studentInfoList.add("{\"name\":\"stud_01\",\"age\":18}");
        studentInfoList.add("{\"name\":\"stud_02\",\"age\":17}");
        studentInfoList.add("{\"name\":\"stud_03\",\"age\":19}");
        studentInfoList.add("{\"name\":\"stud_04\",\"age\":20}");
        studentInfoList.add("{\"name\":\"stud_05\",\"age\":21}");
        JavaRDD<String> studentInfoListRDD = javaSparkContext.parallelize(studentInfoList);
        Dataset<Row> studentInfoDataset = sqlContext.read().json(studentInfoListRDD);
        studentInfoDataset.registerTempTable("student_infos");

        //查询出分数在80以上的学生姓名和成绩和年龄
        Dataset<Row> goodStudentScoreAgeDataset = sqlContext.sql("select a.name, a.score, b.age from student_score a, student_infos b where a.name = b.name and a.score >= 80");
        goodStudentScoreAgeDataset.toJavaRDD().foreach(new VoidFunction<Row>() {
            public void call(Row row) throws Exception {
                String name = row.get(0).toString();
                Integer score = Integer.parseInt(row.get(1).toString());
                Integer age = Integer.parseInt(row.get(2).toString());
                System.out.println("name:" + name + "\t" + "score:" + score + "\t" + "age:" + age);
            }
        });
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(SparkSqlJsonExample.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        SparkSqlJsonExample sparkSqlJsonExample = new SparkSqlJsonExample();
//        sparkSqlJsonExample.doit(javaSparkContext);
//        sparkSqlJsonExample.doit2(javaSparkContext);
        sparkSqlJsonExample.doit3(javaSparkContext);
        javaSparkContext.close();
    }
}
