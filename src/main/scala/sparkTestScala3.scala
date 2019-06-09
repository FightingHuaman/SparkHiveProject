import java.util
import java.util.Properties

import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
case class UserNetTtrace3(
                          device_number:String,
                          lac :String,
                          ci:String,
                          imei:String,
                          dpi_type:String,
                          start_time:String,
                          end_time:String,
                          cdr_dura:String,
                          up_flow:String,
                          down_flow:String,
                          total_flow:String,
                          rat_type:String,
                          terminal_ip:String,
                          target_ip:String,
                          status_code:String,
                          user_agent:String,
                          apn:String,
                         imsi:String,
                          sgsn_ip:String,
                          ggsn_ip:String,
                          content_type:String,
                          source_port:String,
                          target_port:String,
                          record_megtype:String,
                          merge_records:String,
                          url:String
                        )
//class sparkTestScala3
object sparkTestScala3 extends Logging{



  def main(args: Array[String]): Unit = {
//   System.setProperty("hadoop.home.dir", "D:\\2345Downloads\\hadoop-common-2.6.0-bin-master"); //?
    try {
          if (args.length!=2)
            {println("useage:inputfile outputfile")
            return
            }
          val spark = SparkSession
            .builder
            .appName("SparkSqlTest")
            .enableHiveSupport()
            //spark运行模式，在提交程序时指定
          .master("local[*]")//?
            .getOrCreate
          spark.sparkContext.setLogLevel("WARN")
          spark.sql("show databases").show(false)//?
          //使用数据库，可配
          //spark.sql("use opdw4_226")
          logInfo(readValue("myconfig.properties","dataBaseName"))
          spark.sql(readValue("myconfig.properties","dataBaseName"))
          import spark.implicits._
          System.out.println("---------------------------------")//?
          val rdd =spark.sparkContext.textFile(args(0))//?
          //val rdd =spark.sparkContext.textFile("file:///wocloud/opdw4_226/data/dfyx.txt")
             val dataframe =rdd.map(_.split("\\|",-1))
                .map(p => UserNetTtrace3(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8), p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16), p(17),p(18),p(19),p(20),p(21),p(22),p(23),p(24),p(25)))
                .toDF()//?
          dataframe.show(false)//?
          dataframe.createOrReplaceTempView("urlTable")
          //log.info(dataframe.printSchema())
          //使用拼接方式实现多个url匹配
      //    val likes = readProperties("/usr/local/myconfig.properties","url")
      //    val keywords = readProperties("/usr/local/myconfig.properties","keyword")
          val likes = readProperties("myconfig.properties","url")
          val keywords = readProperties("myconfig.properties","keyword")
      //    SELECT DISTINCT pi_num.device_number,pi_num.url,pi_num.ct,pi_num.month_id ，4个
          val  sql ="SELECT DISTINCT pi_num.device_number,pi_num.url,pi_num.ct,pi_num.month_id " +
            "FROM\n(\nSELECT info.user_id,info.prov_id,p_d.device_number,p_d.url,p_d.ct,info.month_id " +
            "FROM\n(\nSELECT p.device_number,p.url,COUNT(*) as ct FROM urlTable p " +
            "WHERE p.url like 'qwerrereff'" +likes+
            "\nGROUP BY p.device_number,p.url )\n as p_d, " +
            "\ndwa_v_m_cus_nm_user_info info WHERE p_d.device_number=info.device_number)\n as pi_num," +
            "\n(\nSELECT d.device_number ,d.cert_age,d.subs_instance_id,d.prov_id " +
          //修改为北京地区，编码为  V0110000  武汉地区为   V0420100
            "FROM dwa_v_m_cus_cb_rns_wide AS d WHERE area_id = 'V0110000' and " +
            "cert_age BETWEEN '20' AND '50'  )\n as d_d\n" +
            "WHERE pi_num.device_number=d_d.device_number and pi_num.prov_id=d_d.prov_id and " +
            "concat(pi_num.user_id,pi_num.prov_id)=concat('A',d_d.subs_instance_id,d_d.prov_id)"
      //    val sql ="select device_number,url from urlTable p where p.url like '%miui.com%' or p.url like '%www.hengqijy.com%' or p.url like '%zikao.hqjy.com%' or p.url like '%hbzkzk.com%' or p.url like '%hbjxjyw.cn%' or p.url like '%zh.xuehuiwang.com%' or p.url like '%ksfwpt.cn%' or p.url like '%hbxlts.com%' or p.url like '%hengqijiaoyu.cn%' or p.url like '%hbjxjyw.com/fwzx/%' or p.url like '%hubei.hengqiedu.cn%' or p.url like '%yingxinshu.com%' or p.url like '%xuehuiwang.com.cn%' or p.url like '%hbjjbk.com%' or p.url like '%bjlhedu.com%' or p.url like '%zh.gzbaiyzc.cn%' or p.url like '%whlgdxw.cn%' or p.url like '%xwlmx.com%' or p.url like '%hbjjzk.com%' or p.url like '%xuehuiwang.com.cn%' or p.url like '%hbjxjy.org%' or p.url like '%zlcjzsb.org/zk%' or p.url like '%static.hbjxjyw.cn%' or p.url like '%ds.sungoedu.com%' or p.url like '%vipzhonghui.cn%' or p.url like '%hbcrjyzsw.com%' or p.url like '%hubeixueli.com%' or p.url like '%whkjdxzsw.cn%' or p.url like '%whkjdxzsw.cn%' or p.url like '%whlgdxw.cn%' or p.url like '%sku.duia.com%' or p.url like '%hbzkw.com%' or p.url like '%ccnuc.com%' or p.url like '%yingxinshu.com%' or p.url like '%m.sunlands.com%' or p.url like '%wendu.com%' or p.url like '%xuehuiwang.com.cn%'"

          println("--------------------this is a sql-----------------")
          logInfo(sql)
          val userUrl = spark.sql(sql)
          userUrl.show(false)//?
          logInfo("获取的数据条数："+userUrl.count())
          println("获取的数据条数："+userUrl.count())
      //    userUrl.repartition(1).write.csv("file:///wocloud/opdw4_226/happyman/output/userUrl.txt")
          userUrl
            .repartition(1)
            .limit(readValue("myconfig.properties","data_limit").toInt)
            //.map(x=>x.toString().replace(",","|"))
            .map(x=>StringUtils.strip(x.toString().replace(",","|"),"[]"))
            .write
          //  .option("header","true")
            .text(args(1))//?


          import spark.implicits._
    }catch
      {
        case exception: Exception =>logError(exception.getMessage)
      }
  }

  import java.io.BufferedInputStream
  import java.io.FileInputStream
  //读取properties的全部信息
  def readProperties(filePath: String,prefix:String): String = {
    var string =""
    val props = new Properties
    try {
      props.load(new FileInputStream(filePath))
      val en = props.propertyNames

      while ( {
        en.hasMoreElements
      }) {
        val key = en.nextElement.asInstanceOf[String]
        if(key.startsWith(prefix))
        {
          val Property = props.getProperty(key)
          println(key +":"+ Property)//?
          string = string+" or p.url like "+"'%"+Property+"%'"
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError(e.getMessage)
    }
    println(string)
   return string
  }


  def readValue(filePath: String, key: String): String = {
    val props = new Properties
    try {
      val in = new BufferedInputStream(new FileInputStream(filePath))
      props.load(in)
      val value = props.getProperty(key)
      System.out.println(key + value)
      value
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError(e.getMessage)
        null
    }
  }
}

