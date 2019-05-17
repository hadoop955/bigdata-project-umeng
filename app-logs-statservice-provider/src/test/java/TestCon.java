import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @Author: skm
 * @Date: 2019/4/24 9:37
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class TestCon {
    @Test
    public void test1() throws SQLException {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("beans.xml");
        DataSource dataSource = (DataSource) applicationContext.getBean("dataSource2");
        Connection connection = dataSource.getConnection();
        System.out.println(connection);


    }
}
