package com.xunmall.example.flume.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/11/4 18:44
 */
public class ElasticsearchSinkV1 extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSink.class);

    private String esHost;
    private String esIndex;
    private static int defaultTimeout = 5_000;

    private TransportClient client;

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            Event event = ch.take();

            if (event != null) {
                String body = new String(event.getBody(), "UTF-8");
                JSONObject json = new JSONObject();
                json.put("message", body);
                BulkRequestBuilder bulkReq = client.prepareBulk();
                for (int i = 0;i < 10; i++) {
                    IndexRequestBuilder indexRequestBuilder = client.prepareIndex().setIndex(esIndex).setType(esIndex);
                    indexRequestBuilder.setId(UUID.randomUUID().toString());
                    indexRequestBuilder.setSource(json);
                    bulkReq.add(indexRequestBuilder);
                }

                BulkResponse response = bulkReq.execute().actionGet(defaultTimeout);
                if (response.hasFailures()) {
                    logger.error("error bulk " + response.buildFailureMessage());
                }

                status = Status.READY;
            } else {
                status = Status.BACKOFF;
            }

            txn.commit();
        } catch (Throwable t) {
            txn.rollback();
            t.getCause().printStackTrace();

            status = Status.BACKOFF;
        } finally {
            txn.close();
        }

        return status;

    }

    @Override
    public void configure(Context context) {
        esHost = context.getString("es_host");
        esIndex = context.getString("es_index");
    }

    @Override
    public synchronized void start() {
        try {
            Settings settings = Settings.builder().put("cluster.name", "my-es").build();
            client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esHost), 9300));
            super.start();

            System.out.println("finish start");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }
}
