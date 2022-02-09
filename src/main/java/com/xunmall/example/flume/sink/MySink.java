package com.xunmall.example.flume.sink;

import com.xunmall.example.flume.monitor.GlobalMonitorHolder;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/10/22 9:40
 */
public class MySink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSink.class);

    private GlobalMonitorHolder globalMonitorHolder;

    private String prefix;
    private String suffix;
    private boolean monitorEnable;
    private String monitorUrl;
    private String kafkaMonitorTag;
    private String esMonitorTag;

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        Long startTime = System.currentTimeMillis();
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        transaction.begin();
        while (true) {
            event = channel.take();
            if (event != null) {
                break;
            }
        }
        try {
            logger.info(prefix + new String(event.getBody()) + suffix);
            transaction.commit();
            status = Status.READY;
        } catch (Exception ex) {
            ex.printStackTrace();
            globalMonitorHolder.updateKafkaException();
            status = Status.BACKOFF;
        } finally {
            transaction.close();
        }
        Long endTime = System.currentTimeMillis();
        globalMonitorHolder.updateKafkaCount();
        globalMonitorHolder.updateKafkaTime(endTime - startTime);
        return status;
    }

    @Override
    public void configure(Context context) {
        configMonitor(context);
    }

    public void configMonitor(Context context){
        prefix = context.getString("prefix", "hello");
        suffix = context.getString("suffix");
        monitorEnable = context.getBoolean("monitorEnable");
        monitorUrl = context.getString("monitorUrl");
        kafkaMonitorTag = context.getString("kafkaMonitorTag");
        esMonitorTag = context.getString("esMonitorTag");
    }


    @Override
    public synchronized void start() {
        if (monitorEnable) {
            logger.info("initGlobalMonitorConfig monitorUrl: {} ", monitorUrl);
            globalMonitorHolder = new GlobalMonitorHolder();
            // 配置相应的监控项，位置1、2、3，1 表示总数 2 表示异常数据 3 表示耗时， 后续可以自定义增加
            // 封装kafka对应sink的数据信息
            if (StringUtils.isNotBlank(kafkaMonitorTag)) {
                String[] kafkaTag = kafkaMonitorTag.split(",");
                if (kafkaTag.length == 3) {
                    globalMonitorHolder.setKafkaCount(kafkaTag[0]);
                    globalMonitorHolder.setKafkaException(kafkaTag[1]);
                    globalMonitorHolder.setKafkaTime(kafkaTag[2]);
                }
            }
            if (StringUtils.isNotBlank(esMonitorTag)) {
                String[] esTag = esMonitorTag.split(",");
                if (esTag.length == 3) {
                    globalMonitorHolder.setEsCount(esTag[0]);
                    globalMonitorHolder.setEsException(esTag[1]);
                    globalMonitorHolder.setEsTime(esTag[2]);
                }
            }
            Preconditions.checkNotNull(monitorUrl, "url must be set!!");
            globalMonitorHolder.getMonitorDataUploader().startMonitor(monitorUrl);
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }
}
