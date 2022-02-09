package com.xunmall.example.flume.kafka;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/10/28 17:47
 */
public class ConsumerGroupOffsetJmxTest {

    private MBeanServerConnection connection;

    private JMXConnector jmxc;

    @Before
    public void init() throws IOException {
        String ipAndPort = "10.241.122.142:9999";
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + ipAndPort + "/jmxrmi");
        jmxc = JMXConnectorFactory.connect(url);
        connection = jmxc.getMBeanServerConnection();
    }

    @After
    public void destroy() throws IOException {
        jmxc.close();
    }

    @Test
    public void testGroupOffset() throws MalformedObjectNameException, IntrospectionException, ReflectionException, InstanceNotFoundException, IOException, AttributeNotFoundException, MBeanException {
        ObjectName mBeanName = new ObjectName("kafka.log:type=Log,name=LogEndOffset,topic=group1027,partition=0");
        // 获取值
        Object value = connection.getAttribute(mBeanName, "Value");
        System.out.println(value);
        // 执行MBean的方法
        Object invoke = connection.invoke(mBeanName, "objectName", null, null);
        System.out.println(invoke);

        MBeanInfo info = connection.getMBeanInfo(mBeanName);
        System.out.println("ClassName：" + info.getClassName());
        for (MBeanAttributeInfo attr : info.getAttributes()) {
            System.out.println("属性：" + attr.getName() + "，类型：" + attr.getType() + "，值：" + connection.getAttribute(mBeanName, attr.getName()));

        }
        for (MBeanOperationInfo op : info.getOperations()) {
            System.out.println("操作：" + op.getName());
        }

    }

    @Test
    public void testJmx() throws Exception {

        System.out.println("=========Domains=========");
        String[] domains = connection.getDomains();
        for (String d : domains) {
            System.out.println(d);
        }

        System.out.println("=========MBeans=========");
        System.out.println(connection.getMBeanCount());


        System.out.println("=========Invoke=========");
        ObjectName mBeanName = new ObjectName("kafka.log:type=Log,name=Size,topic=topic1028,partition=0");
        // 获取值
        Object value = connection.getAttribute(mBeanName, "Value");
        System.out.println(value);
        // 执行MBean的方法
        Object invoke = connection.invoke(mBeanName, "objectName", null, null);
        System.out.println(invoke);


        System.out.println("=========MBean Info=========");
        mBeanName = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec");
        MBeanInfo info = connection.getMBeanInfo(mBeanName);
        System.out.println("ClassName：" + info.getClassName());
        for (MBeanAttributeInfo attr : info.getAttributes()) {
            System.out.println("属性：" + attr.getName() + "，类型：" + attr.getType() + "，值：" + connection.getAttribute(mBeanName, attr.getName()));

        }
        for (MBeanOperationInfo op : info.getOperations()) {
            System.out.println("操作：" + op.getName());
        }
    }


}
