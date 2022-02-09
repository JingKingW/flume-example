package com.xunmall.example.flume.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;

/**
 * @author wangyanjing
 * @date 2020/8/19
 */
public class ProducerFastTest {

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerFastTest.brokerList);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.1103");
        return properties;
    }

    /**
     * 实践基础kafka生产端功能
     */
    @Test
    public void testSimpleStart() throws InterruptedException {
        Properties properties = initConfig();

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord record = null;
        // 异步发送多条消息
        for (int i = 0; i < 10000; i++) {
            record = new ProducerRecord(ConsumerFastTest.topic, "msg" + i);
            Thread.sleep(new Random().nextInt(100));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        // 增加正常的容错记录信息
                        e.printStackTrace();
                    } else {
                        System.out.println("topic :" + recordMetadata.topic() + "--- partition--" + recordMetadata.partition() + " --offset:" + recordMetadata.offset());
                    }
                }
            });
        }
        producer.close();
    }

}
