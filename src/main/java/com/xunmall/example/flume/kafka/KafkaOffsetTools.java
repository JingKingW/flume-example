package com.xunmall.example.flume.kafka;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.network.BlockingChannel;

import java.util.*;
import java.util.Map.Entry;

public class KafkaOffsetTools {

    public static void main(String[] args) {

        String topic = "topic1102";
        String broker = "10.241.122.142";
        int port = 9092;
        String group = "group1102";
        String clientId = "clientid1027";
        int correlationId = 0;
        BlockingChannel channel = new BlockingChannel(broker, port,
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                5000);
        channel.connect();

        List<String> seeds = new ArrayList<String>();
        seeds.add(broker);
        KafkaOffsetTools kot = new KafkaOffsetTools();

        TreeMap<Integer, PartitionMetadata> metadatas = kot.findLeader(seeds, port, topic);

        long sum = 0L;
        long sumOffset = 0L;
        long lag = 0L;
        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        for (Entry<Integer, PartitionMetadata> entry : metadatas.entrySet()) {
            int partition = entry.getKey();
            TopicAndPartition testPartition = new TopicAndPartition(topic, partition);
            partitions.add(testPartition);
        }
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                group,
                partitions,
                (short) 0,
                correlationId,
                clientId);
        for (Entry<Integer, PartitionMetadata> entry : metadatas.entrySet()) {
            int partition = entry.getKey();
            try {
                channel.send(fetchRequest.underlying());
                OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload());
                TopicAndPartition testPartition0 = new TopicAndPartition(topic, partition);
                OffsetMetadataAndError result = fetchResponse.offsets().get(testPartition0);
                short offsetFetchErrorCode = result.error();
                if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
                } else {
                    long retrievedOffset = result.offset();
                    sumOffset += retrievedOffset;
                }
                String leadBroker = entry.getValue().leader().host();
                String clientName = "Client_" + topic + "_" + partition;
                SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000,
                        64 * 1024, clientName);
                long readOffset = getLastOffset(consumer, topic, partition,
                        kafka.api.OffsetRequest.LatestTime(), clientName);
                sum += readOffset;
                System.out.println(partition + ":" + readOffset);
                if (consumer != null) consumer.close();
            } catch (Exception e) {
                channel.disconnect();
            }
        }

        System.out.println("logSize：" + sum);
        System.out.println("offset：" + sumOffset);

        lag = sum - sumOffset;
        System.out.println("lag:" + lag);


    }

    public KafkaOffsetTools() {
    }


    public static long getLastOffset(SimpleConsumer consumer, String topic,
                                     int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
                partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            System.out
                    .println("Error fetching data Offset Data the Broker. Reason: "
                            + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private TreeMap<Integer, PartitionMetadata> findLeader(List<String> a_seedBrokers,
                                                           int a_port, String a_topic) {
        TreeMap<Integer, PartitionMetadata> map = new TreeMap<Integer, PartitionMetadata>();
        loop:
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024,
                        "leaderLookup" + System.currentTimeMillis());
                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        map.put(part.partitionId(), part);
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed
                        + "] to find Leader for [" + a_topic + ", ] Reason: " + e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
        return map;
    }

}
