package com.xunmall.example.flume.monitor;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/11/10 14:38
 */
public class GlobalMonitorHolder {

    private MonitorDataUploader monitorDataUploader = new DefaultMonitorDataUploader();

    private String kafkaCount;

    private String kafkaException;

    private String kafkaTime;

    private String esCount;

    private String esException;

    private String esTime;

    public MonitorDataUploader getMonitorDataUploader() {
        return monitorDataUploader;
    }

    public void setMonitorDataUploader(MonitorDataUploader monitorDataUploader) {
        this.monitorDataUploader = monitorDataUploader;
    }

    public String getKafkaCount() {
        return kafkaCount;
    }

    public void setKafkaCount(String kafkaCount) {
        this.kafkaCount = kafkaCount;
    }

    public String getKafkaException() {
        return kafkaException;
    }

    public void setKafkaException(String kafkaException) {
        this.kafkaException = kafkaException;
    }

    public String getKafkaTime() {
        return kafkaTime;
    }

    public void setKafkaTime(String kafkaTime) {
        this.kafkaTime = kafkaTime;
    }

    public String getEsCount() {
        return esCount;
    }

    public void setEsCount(String esCount) {
        this.esCount = esCount;
    }

    public String getEsException() {
        return esException;
    }

    public void setEsException(String esException) {
        this.esException = esException;
    }

    public String getEsTime() {
        return esTime;
    }

    public void setEsTime(String esTime) {
        this.esTime = esTime;
    }

    public void updateKafkaCount(){
        monitorDataUploader.setValue(getKafkaCount(),1,MonitorDataType.INCREMENT);
    }

    public void updateKafkaException(){
        monitorDataUploader.setValue(getKafkaException(),1,MonitorDataType.INCREMENT);
    }

    public void updateKafkaTime(Long time){
        monitorDataUploader.setValue(getKafkaTime(),time,MonitorDataType.INCREMENT);
    }

    public void updateEsCount(){
        monitorDataUploader.setValue(getEsCount(),1,MonitorDataType.INCREMENT);
    }

    public void updateEsException(){
        monitorDataUploader.setValue(getEsException(),1,MonitorDataType.INCREMENT);
    }

    public void updateEsTime(Long time){
        monitorDataUploader.setValue(getEsTime(),time,MonitorDataType.INCREMENT);
    }

}
