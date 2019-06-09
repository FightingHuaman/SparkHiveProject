import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.*;
public class hiveSql {
    public static  void main(String args[]) throws  Exception {
        //System.setProperty("hadoop.home.dir", "D:\\\\2345Downloads\\\\hadoop-common-2.6.0-bin-master");
        SparkSession sparkSql = SparkSession
                .builder()
                .appName("hiveSql")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate();
        //spark.sql("show databases").show();
        sparkSql.sparkContext().setLogLevel("ERROR");
        sparkSql.sql("use zba_dwa");
       //读表的形式
        String sql ="select info.* from(select * FROM(select b.device_number as web_num,count(0) as web from web_app as a INNER JOIN dwa_m_ia_basic_user_app as b where a.web_app=b.prod_name group by device_number) as tab_web inner join( select b.device_number as pos_num,count(0) as pos from pos_app as a INNER JOIN dwa_m_ia_basic_user_app as b where a.pos_app=b.prod_name group by device_number) as tab_pos on tab_web.web_num=tab_pos.pos_num ) as rs, dwa_v_m_cus_cb_rns_wide as info where rs.web>=1 and rs.pos>=2 and rs.web_num=info.device_number and info.is_this_acct=1";
        sparkSql.sql(sql).show();
        long   s=sparkSql.sql(sql).count();
        System.out.println(s);

    }
}
