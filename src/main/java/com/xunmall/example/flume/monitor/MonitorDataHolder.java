package com.xunmall.example.flume.monitor;

import org.apache.commons.collections.KeyValue;
import org.apache.commons.collections.keyvalue.DefaultKeyValue;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 存放监控数据的容器类
 *
 * @author wangyj03
 */
public class MonitorDataHolder {

    /**
     * 用以存放请求数量
     */
    private Map<String, AtomicInteger> counters = new ConcurrentHashMap<String, AtomicInteger>();

    /**
     * 用以存放计算平均数的中间数据
     */
    private Map<String, AvgRawData> averageCounters = new ConcurrentHashMap<String, AvgRawData>();

    /**
     * 用以存放最大值
     */
    private Map<String, AtomicLong> maxValues = new ConcurrentHashMap<String, AtomicLong>();

    /**
     * 用以存放计算 ratio 的原始值, 目前只支持因子、基数都为 int 来统计 ratio
     */
    private Map<String, KeyValue> ratioCounters = new ConcurrentHashMap<String, KeyValue>();

    /**
     * 递增统计接口
     * 初始化时由于并发可能会产生非常微小的数据误差
     */
    public void increment(String key, int intValue) {
        AtomicInteger counter = counters.get(key);
        if (counter == null) {
            counter = new AtomicInteger(0);
            counters.put(key, counter);
        }
        counter.addAndGet(intValue);
    }

    /**
     * 平均值统计接口
     */
    public void insert(String key, double doubleValue) {
        AvgRawData counter = averageCounters.get(key);
        if (counter == null) {
            counter = new AvgRawData();
            averageCounters.put(key, counter);
        }
        counter.add((long) doubleValue);
    }

    /**
     * 最大值统计接口
     */
    public void setMax(String key, double newValue) {
        setMaxNoBlock(key, (long) newValue);
    }

    /**
     * 不用锁，但极端情况下可能会设置失效
     *
     * @param key
     * @param newValue
     */
    public void setMaxNoBlock(String key, long newValue) {
        AtomicLong value = maxValues.get(key);
        if (value == null) {
            maxValues.put(key, new AtomicLong(newValue));
        } else {
            long v = value.get();
            if (newValue > v) {
                for (int i = 0; i < 5; i++) {
                    if (value.compareAndSet(v, newValue) || newValue < (v = value.get())) {
                        break;
                    }
                }
            }
        }
    }

    /**
     * ratio 统计递增因子
     */
    public void incrementRatioFactor(String key, int value, boolean incrBase) {
        KeyValue keyValue = ratioCounters.get(key);
        if (keyValue == null) {
            keyValue = new DefaultKeyValue(new AtomicInteger(0), new AtomicInteger(0));
            ratioCounters.put(key, keyValue);
        }
        synchronized (key.intern()) {
            ((AtomicInteger) keyValue.getKey()).incrementAndGet();
            if (incrBase) {
                ((AtomicInteger) keyValue.getValue()).incrementAndGet();
            }
        }
    }

    /**
     * ratio 统计递增基数
     */
    public void incrementRatioBase(String key, int value) {
        KeyValue keyValue = ratioCounters.get(key);
        if (keyValue == null) {
            keyValue = new DefaultKeyValue(new AtomicInteger(0), new AtomicInteger(0));
            ratioCounters.put(key, keyValue);
        }
        synchronized (key.intern()) {
            ((AtomicInteger) keyValue.getValue()).incrementAndGet();
        }
    }

    public Map<String, AtomicInteger> getCounters() {
        return counters;
    }

    public Map<String, Double> getAverageValues() {
        Map<String, Double> averageValues = new HashMap<String, Double>(averageCounters.size());
        DecimalFormat format = new DecimalFormat("#.#");
        for (Entry<String, AvgRawData> entry : averageCounters.entrySet()) {
            averageValues.put(entry.getKey(), Double.parseDouble(format.format(entry.getValue().get())));
        }
        return averageValues;
    }

    public Map<String, AtomicLong> getMaxValues() {
        return maxValues;
    }

    public Map<String, Double> getRatios() {
        Map<String, Double> ratioValues = new HashMap<String, Double>(ratioCounters.size());
        DecimalFormat format = new DecimalFormat("#.###");
        for (Entry<String, KeyValue> entry : ratioCounters.entrySet()) {
            synchronized (entry.getKey().intern()) {
                KeyValue keyValue = entry.getValue();
                Double ratio = 0d;
                int factor = ((AtomicInteger) keyValue.getKey()).get();
                int base = ((AtomicInteger) keyValue.getValue()).get();
                if (base != 0) {
                    ratio = (new Double(factor) / new Double(base)) * 100;
                    ratio = Double.parseDouble(format.format(ratio));
                }
                ratioValues.put(entry.getKey(), ratio);
            }
        }
        return ratioValues;
    }

    // XXX 有比较轻微的并发问题
    public void reset() {
        for (Entry<String, AtomicInteger> entry : counters.entrySet()) {
            entry.getValue().set(0);
        }
        for (String key : maxValues.keySet()) {
            maxValues.put(key, new AtomicLong());
        }
        for (String key : averageCounters.keySet()) {
            // Queue<Double> newQueue = new ConcurrentLinkedQueue<>();
            averageCounters.put(key, new AvgRawData());
        }
        for (Entry<String, KeyValue> entry : ratioCounters.entrySet()) {
            synchronized (entry.getKey().intern()) {
                KeyValue keyValue = entry.getValue();
                ((AtomicInteger) keyValue.getKey()).set(0);
                ((AtomicInteger) keyValue.getValue()).set(0);
            }
        }
    }

    class AvgRawData {
        AtomicLong total = new AtomicLong();
        AtomicLong count = new AtomicLong();

        void add(long data) {
            total.getAndAdd(data);
            count.getAndIncrement();
        }

        double get() {
            if (count.get() == 0L) {
                return 0.0d;
            }
            return (double) total.get() / count.get();
        }
    }
}
