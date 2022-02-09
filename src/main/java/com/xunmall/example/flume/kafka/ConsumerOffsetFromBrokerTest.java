package com.xunmall.example.flume.kafka;

import kafka.common.OffsetAndMetadata;
import kafka.coordinator.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/11/6 10:16
 */
public class ConsumerOffsetFromBrokerTest {

    public static final String brokerList = "10.241.122.142:9092";

    public static final String topic = "__consumer_offsets";

    public static final String groupId = "monitor-offset";

    public static final String clientId = "monitor-client-id";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        //设置多久一次更新被消费消息的偏移量
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        return properties;
    }

    @Test
    public void testConsumerSimple() {
        Properties properties = initConfig();

        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<byte[], byte[]>(properties);

        // 订阅主题消息
        kafkaConsumer.subscribe(Arrays.asList(topic));

        try {
            while (true) {
                ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(10000);
                if (!consumerRecords.isEmpty()) {
                    for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                        BaseKey key = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key()));
                        String topic = null;
                        String group = null;
                        if (key instanceof OffsetKey) {
                            GroupTopicPartition partition = (GroupTopicPartition) key.key();
                            group = partition.group();
                            TopicPartition topicPartition = partition.topicPartition();
                            topic = topicPartition.topic();
                            if ("flume-kafka-es".equals(topic) && "group1103".equals(group)) {
                                OffsetAndMetadata om = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(record.value()));
                                long oofset = om.offset();
                                System.out.println("group : " + group + "  topic : " + topic + "  partition : "+  topicPartition.partition() +"   oofset ： " + oofset);
                            }
                        }
                        if (key instanceof GroupMetadataKey) {
                            System.out.println("groupMetadataKey: " + key.key());
                            //第一个参数为group id，先将key转换为GroupMetadataKey类型，再调用它的key()方法就可以获得group id
                            GroupMetadata groupMetadata = GroupMetadataManager.readGroupMessageValue(((GroupMetadataKey) key).key(), ByteBuffer.wrap(record.value()));

                            System.out.println("GroupMetadata: "+groupMetadata.toString());
                        }
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }

}
