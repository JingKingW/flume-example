package com.xunmall.example.flume.kafka;

import kafka.admin.ConsumerGroupCommand;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/10/28 14:17
 */
public class MonitorGroupOffsetTest {

    private static final String bootstrap = "10.241.122.142:9092";

    public static Properties createConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1028");
        properties.put("bootstrap.servers", bootstrap);
        return properties;
    }

    @Test
    public void loadPartitionOffsets() {
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(createConsumerProperties());
        List<TopicPartition> topicPartitions = consumer.partitionsFor("topic1103")
                .stream()
                .map(t -> new TopicPartition(t.topic(), t.partition()))
                .collect(Collectors.toList());
        // 这里其实就是在求visible-offset
        Map<TopicPartition, Long> map = consumer.endOffsets(topicPartitions);
        map.forEach((key, value) -> {
            System.out.println(key.partition() + " :  " + value);
        });
    }


    @Test
    public void testGroupLag(){
        String[] options1 = new String[]{"--bootstrap-server","10.241.122.142:9092","--new-consumer","--list"};
        kafka.admin.ConsumerGroupCommand.main(options1);
        System.out.println("====================================");
        String[] options2 = new String[]{"--bootstrap-server","10.241.122.142:9092","--new-consumer","--describe","--group","group1103"};
        kafka.admin.ConsumerGroupCommand.main(options2);
        System.out.println("====================================");
        String[] options3 = new String[]{"--zookeeper","10.241.122.142:2181","--list"};
        kafka.admin.ConsumerGroupCommand.main(options3);
        System.out.println("====================================");
        String[] options4 = new String[]{"--zookeeper","10.241.122.142:2181","--describe","--group","group1103"};
        kafka.admin.ConsumerGroupCommand.main(options4);

    }

    @Test
    public void testGroupLagByTopic(){
        String bootstrap = "10.241.122.142:9092";
        String consumerGroup = "group1103";
        String topic ="flume-kafka-es";
        KafkaConsumer consumer = createKafkaConsumer(bootstrap ,consumerGroup );
        String[] agrs = {"--describe", "--bootstrap-server", bootstrap, "--group", consumerGroup};
        ConsumerGroupCommand.ConsumerGroupCommandOptions opts = new ConsumerGroupCommand.ConsumerGroupCommandOptions(agrs);
        ConsumerGroupCommand.KafkaConsumerGroupService kafkaConsumerGroupService = new ConsumerGroupCommand.KafkaConsumerGroupService(opts);
        List<PartitionInfo> topicPartitions = consumer.partitionsFor(topic);
        topicPartitions.forEach(item->{
            TopicPartition topicPartition = new TopicPartition(topic, item.partition());
            ConsumerGroupCommand.LogEndOffsetResult offSetResult =kafkaConsumerGroupService.getLogEndOffset(topicPartition);
            String offval = offSetResult.toString();

            Long partitionOffset = Long.valueOf(offval.replace("LogEndOffset(","").replace(")",""));
            System.out.println("partitionId  = "+item.partition()+",partitionOffset = "+partitionOffset);

            List<TopicPartition> listTopicPartition = new ArrayList<>();
            listTopicPartition.add(topicPartition);
            consumer.assign(listTopicPartition);

            long groupOffset = consumer.position(topicPartition);

            System.out.println("partitionId = "+item.partition()+",groupOffset="+groupOffset);

            long lag = partitionOffset - groupOffset ;
            System.out.println( "group : " + consumerGroup + " partitionId : " + item .partition() + " Lag : " + lag);

        });
        kafkaConsumerGroupService.close();
    }

    private KafkaConsumer createKafkaConsumer(String broker, String groupId){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        properties.put("enable.auto.commit", "false");
        return new KafkaConsumer(properties);
    }
}
