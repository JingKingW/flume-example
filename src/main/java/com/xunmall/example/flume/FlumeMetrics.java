package com.xunmall.example.flume;

import org.apache.flume.Context;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/10/22 10:52
 */
public class FlumeMetrics implements MonitorService {

    private static final Logger logger = LoggerFactory.getLogger(FlumeMetrics.class);

    private ScheduledExecutorService scheduledExecutorService;

    /**
     * 一些相关配置可以写在这里面，这个方法会先执行
     * 也就是开始监控前的一些操作就可以写到这里面
     */
    @Override
    public void configure(Context context) {
        System.out.println("configure ........");
    }

    /**
     * 监控开始，调用我们自定义的监控任务，比如收集的指标，发送到哪里
     * 比如我是20秒收集一次指标，打印到控制台
     */
    @Override
    public void start() {
        System.out.println("start ........");
        MetricsCollector collector = new MetricsCollector();
        if (scheduledExecutorService == null || scheduledExecutorService.isShutdown() || scheduledExecutorService.isTerminated()) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        }
        scheduledExecutorService.scheduleWithFixedDelay(collector, 0,
                10000, TimeUnit.MILLISECONDS);
    }

    class MetricsCollector implements Runnable {

        @Override
        public void run() {
            String ip = getHostIp();
            logger.info("Target IP : {}" , ip);
            try {
                long startTime = System.currentTimeMillis();
                Map<String, Map<String, String>> metricsMap = JMXPollUtil.getAllMBeans();
                logger.info("Metrics Map size: {}",metricsMap.size());
                for (String component : metricsMap.keySet()) {
                    Map<String, String> attributeMap = metricsMap.get(component);
                    System.out.println("============分隔component================");
                    System.out.println("Attributes for component " + component);
                    for (String key : attributeMap.keySet()) {
                        System.out.println("attribute:value=" + key + ":" + attributeMap.get(key));
                    }
                }
                long endTime = System.currentTimeMillis();
                logger.info("exec metrics Time : {}", (endTime - startTime));
            } catch (Exception e) {
                System.out.println("Unexpected error" + e.getMessage());
            }
            System.out.println("Finished collecting Metrics for Flume");
            System.out.println("%%%%%%%%%%%%%%%分隔每一次采集%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
        }
    }

    /**
     * 停止监控，agent停止或退出的时候会自动调用
     */
    @Override
    public void stop() {
        System.out.println("stop ........");
    }

    public static String getHostIp() {
        String address = "";
        try {
            address = (InetAddress.getLocalHost()).getHostAddress();
        } catch (UnknownHostException uhe) {
            address = uhe.getMessage();
            if (address != null) {
                int colon = address.indexOf(':');
                if (colon > 0) {
                    address.substring(0, colon);
                }
            }
        }
        return address;
    }



}
