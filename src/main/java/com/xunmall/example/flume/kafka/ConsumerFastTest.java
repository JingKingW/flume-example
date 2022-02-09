package com.xunmall.example.flume.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author WangYanjing
 * @date 2020/8/19.
 */
public class ConsumerFastTest {

    public static final String brokerList = "10.241.122.142:9092";

    public static final String topic = "flume-kafka-es";

    public static final String groupId = "group1103";

    public static final String clientId = "clientid1103";

    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        return properties;
    }


    /**
     * @Description: [kafka消费端的简单样例]
     * @Title: testConsumerSimple
     * @Author: WangYanjing
     * @Date: 2020/8/21
     * @Param:
     * @Return: void
     * @Throws:
     */
    @Test
    public void testConsumerSimple() {
        Properties properties = initConfig();

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // 订阅主题消息
        kafkaConsumer.subscribe(Arrays.asList(topic));

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + " , offset = " + record.offset());
                    System.out.println("key = " + record.key() + " , value = " + record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}
