package com.xunmall.example.flume.monitor;

public interface MonitorDataUploader {

	void startMonitor(String monitorUrl);

	void setValue(String key, Number value, MonitorDataType monitorDataType, String province);
	
	void setValue(String key, Number value, MonitorDataType monitorDataType);
	
	void upload();

	/**
	 * 递增设置 ratio 监控值
	 */
	@Deprecated
	void setRatioValue(String key, int value, Ratio ratio);
	
	/**
	 * 递增设置 ratio 因子
	 * @param incrBase 是否同步设置因母
	 */
	void incrRatioFactor(String key, int value, boolean incrBase);
	
	/**
	 * 递增设置 ratio 因母
	 */
	void incrRatioBase(String key, int value);
	
	/**
	 * 直接上报指定监控项对应的监控值
	 * 频率由应用方决定，此方法只负责上报数据
	 */
	void upload(String key, Number value);
	
	/**
	 * 直接上报指定监控项对应的监控值
	 * 频率由应用方决定，此方法只负责上报数据
	 */
	void upload(String key, long value, String host);

	/**
	 * 不阻塞set max
	 * @param key
	 * @param value
	 */
	void setMaxNoBlock(String key, long value);
}
