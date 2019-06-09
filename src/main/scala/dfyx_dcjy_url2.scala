import java.io.{File, FileOutputStream, PrintStream}
import java.util
import java.util.Properties

import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
case class UserNetTtrace8(
                          device_number:String, lac :String,
                          ci:String, imei:String,
                          dpi_type:String, start_time:String,
                          end_time:String, cdr_dura:String,
                          up_flow:String, down_flow:String,
                          total_flow:String, rat_type:String,
                          terminal_ip:String, target_ip:String,
                          status_code:String, user_agent:String,
                          apn:String, imsi:String,
                          sgsn_ip:String, ggsn_ip:String,
                          content_type:String, source_port:String,
                          target_port:String, record_megtype:String,
                          merge_records:String, url:String
                        )
object dfyx_dcjy_url2 extends Logging{

  def main(args: Array[String]): Unit = {
    try {
      val f=new File("dfyx_out.txt");
      f.createNewFile
      val  fileOutputStream = new FileOutputStream(f);
      var printStream = new PrintStream(fileOutputStream);
      System.setOut(printStream);
          if (args.length!=2)
            {
              println("useage:inputfile outputfile")
              return
            }
          val spark = SparkSession
            .builder
            .appName("dfyx_dcjy_url")
            .enableHiveSupport()
            //spark运行模式，在提交程序时指定master模式
            .getOrCreate
          spark.sparkContext.setLogLevel("INFO")
          println("当前数据库显示："+spark.sql("show databases").show(false).toString)
              //?
          //使用数据库，可配
          println("使用数据库名："+readValue("myconfig.properties","dataBaseName"))
          spark.sql(readValue("myconfig.properties","dataBaseName"))
          import spark.implicits._

          //args(0) 为输入文件路径
          val inputfile = args(0)
          //args(1) 为输出文件路径
          val  outputfile = args(1)

          //读取输入文件
          val rdd =spark.sparkContext.textFile(inputfile)
          //依次把txt数据映射成dataframe p(0),p(1),p(2),p(3),p(4),p(5)...为依次对应的字段名
          val dataframe =rdd.map(_.split("\\|",-1))
              .map(p => UserNetTtrace8(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8), p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16), p(17),p(18),p(19),p(20),p(21),p(22),p(23),p(24),p(25)))
              .toDF()
          println("原始数据：")//?
          dataframe.show(false)
          dataframe.createOrReplaceTempView("urlTable")
          val  sql =readValue(filePath = "myconfig.properties","sql")
          println("当前sql语句："+sql)
          val userUrl = spark.sql(sql)
          userUrl.show(false)
          println("结果数据：")
          userUrl.show(false)
          println("获取的数据条数："+userUrl.count())
          userUrl
            .repartition(1)
            .limit(readValue("myconfig.properties","data_limit").toInt)
            .map(x=>StringUtils.strip(x.toString().replace(",","|"),"[]"))
            .write
            .text(outputfile)
    }catch
      {
        case exception: Exception =>exception.printStackTrace()
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
          logInfo("配置信息："+key +":"+ Property)//?
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
      logInfo("配置信息："+key +":"+ value)
      value
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError(e.getMessage)
        null
    }
  }
}

