import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

public class propertiesTest
{
    public static void main(String[] args)
    {
       readProperties("myconfig.properties");
    }


    public static void readProperties(String filePath) {
        Properties props = new Properties();
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(filePath));
            props.load(in);
            Enumeration en = props.propertyNames();
            while (en.hasMoreElements()) {
                String key = (String) en.nextElement();
                String Property = props.getProperty (key);
                System.out.println(key+":"+Property);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
