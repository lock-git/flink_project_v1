package com.lock.entry;

public class KafkaLogMsg {
    private String appName;
    private Long submitTime;
    private Long validTo;
    private String data;
    private Integer ishbt;
    private String ip;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public Long getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Long submitTime) {
        this.submitTime = submitTime;
    }

    public Long getValidTo() {
        return validTo;
    }

    public void setValidTo(Long validTo) {
        this.validTo = validTo;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Integer getIshbt() {
        return ishbt;
    }

    public void setIshbt(Integer ishbt) {
        this.ishbt = ishbt;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
