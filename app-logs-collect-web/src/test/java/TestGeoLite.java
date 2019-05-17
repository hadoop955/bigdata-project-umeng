import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.Reader;
import com.skm.app.util.GeoUtil;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;

/**
 * @Author: skm
 * @Date: 2019/4/9 20:02
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class TestGeoLite {
    @Test
    public void test1() throws IOException {
      InputStream in =  ClassLoader.getSystemResourceAsStream("GeoLite2-City.mmdb");
      Reader r = new Reader(in);
      JsonNode node = r.get(InetAddress.getByName("2a01:4f9:2a:185f::2"));
      //获得国家
        String country = node.get("country").get("names").get("zh-CN").textValue();
        //获得省份
        String area = node.get("subdivisions").get(0).get("names").get("zh-CN").textValue();
        //获得城市
        String city = node.get("city").get("names").get("zh-CN").textValue();
        System.out.println(country+"."+area+"."+city);
    }
    @Test
    public void test2() throws IOException {
        System.out.println( GeoUtil.getCountry("49.51.10.192"));
    }
}
