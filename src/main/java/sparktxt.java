import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class sparktxt {
    public static  void main(String args[]) throws  Exception {
       // System.setProperty("hadoop.home.dir", "D:\\2345Downloads\\hadoop-common-2.6.0-bin-master");
        SparkSession sparkSql = SparkSession
                .builder()
                .appName("sparktxt")
                .master("local[2]")
                .getOrCreate();
       // DataFrame urlData=[name: string, age: int]
        Dataset<Row> df = sparkSql.read().text("url.txt");

        df.createOrReplaceTempView("url");
//        Dataset<Row> sqlDF = sparkSql.sql("SELECT device_number FROM global_temp.url");
//        sqlDF.show();

    }
}
