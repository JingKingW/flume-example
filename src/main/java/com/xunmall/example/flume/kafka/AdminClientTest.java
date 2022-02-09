package com.xunmall.example.flume.kafka;

import kafka.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.*;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/10/23 16:57
 */
public class AdminClientTest {

    private static final Logger logger = LoggerFactory.getLogger(AdminClientTest.class);

    private static final String bootstrap = "10.241.122.142:9092";

    public static Properties createProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap);
        return properties;
    }

    @Test
    public void testListAllGroup() {
        Properties properties = createProperties();
        AdminClient adminClient = AdminClient.create(properties);
        Set<String> consumerGroupSet = new HashSet<>();
        scala.collection.immutable.Map<org.apache.kafka.common.Node, scala.collection.immutable.List<kafka.coordinator.GroupOverview>> brokerGroupMap = adminClient.listAllGroups();
        for (scala.collection.immutable.List<kafka.coordinator.GroupOverview> brokerGroup : JavaConversions.asJavaMap(brokerGroupMap).values()) {
            List<kafka.coordinator.GroupOverview> lists = JavaConversions.asJavaList(brokerGroup);
            for (kafka.coordinator.GroupOverview groupOverview : lists) {
                String consumerGroup = groupOverview.groupId();
                if (consumerGroup != null && consumerGroup.contains("#")) {
                    consumerGroup = consumerGroup.split("#", 2)[1];
                }
                consumerGroupSet.add(consumerGroup);
            }
        }
        System.out.println(consumerGroupSet.size());
    }

    @Test
    public void testListGroupOffset() {
        Properties properties = createProperties();
        AdminClient adminClient = AdminClient.create(properties);
        Map<TopicPartition, Object> topicPartitionAndOffsetMap = JavaConversions.asJavaMap(adminClient.listGroupOffsets("group1029"));
        topicPartitionAndOffsetMap.forEach((key, value) -> {
            System.out.println(key.partition() + "  :  " + value);
        });
    }

}
