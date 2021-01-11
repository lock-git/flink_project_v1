package com.lock.entry;

import java.util.List;

public class KafkaLogData {
    public String deviceid;
    public String uid;
    public String plateform;
    public String subplateform;
    public String version;
    public String channel;
    public String client;
    public String os;
    public List<Logs> logs;
    public String net;
    public String ip;

    public String getDeviceid() {
        return deviceid;
    }

    public void setDeviceid(String deviceid) {
        this.deviceid = deviceid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getPlateform() {
        return plateform;
    }

    public void setPlateform(String plateform) {
        this.plateform = plateform;
    }

    public String getSubplateform() {
        return subplateform;
    }

    public void setSubplateform(String subplateform) {
        this.subplateform = subplateform;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public List<Logs> getLogs() {
        return logs;
    }

    public void setLogs(List<Logs> logs) {
        this.logs = logs;
    }

    public String getNet() {
        return net;
    }

    public void setNet(String net) {
        this.net = net;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public static class Logs {
        public String uid;
        public String eventid;
        public String eventcontent;
        public String begintime;
        public String pmenu;
        public String menu;
        public String ip;
        public String net;
        public String lon;
        public String lat;
        public String areacode;
        public String address;

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getEventid() {
            return eventid;
        }

        public void setEventid(String eventid) {
            this.eventid = eventid;
        }

        public String getEventcontent() {
            return eventcontent;
        }

        public void setEventcontent(String eventcontent) {
            this.eventcontent = eventcontent;
        }

        public String getBegintime() {
            return begintime;
        }

        public void setBegintime(String begintime) {
            this.begintime = begintime;
        }

        public String getPmenu() {
            return pmenu;
        }

        public void setPmenu(String pmenu) {
            this.pmenu = pmenu;
        }

        public String getMenu() {
            return menu;
        }

        public void setMenu(String menu) {
            this.menu = menu;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getNet() {
            return net;
        }

        public void setNet(String net) {
            this.net = net;
        }

        public String getLon() {
            return lon;
        }

        public void setLon(String lon) {
            this.lon = lon;
        }

        public String getLat() {
            return lat;
        }

        public void setLat(String lat) {
            this.lat = lat;
        }

        public String getAreacode() {
            return areacode;
        }

        public void setAreacode(String areacode) {
            this.areacode = areacode;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }
    }


    public static class Ctr {
        public String reality_id;
        public String reality_type;
        public String reality_name;
        public String page_id;
        public String exposure_times;
        public String click_times;
        public String res_ctr;
        public String plateform;
        public String device_id;
        public String begintime;
        public String favorite_times;
        public String praise_times;
        public String reply_times;
        public String context;

        public String getReality_id() {
            return reality_id;
        }

        public void setReality_id(String reality_id) {
            this.reality_id = reality_id;
        }

        public String getReality_type() {
            return reality_type;
        }

        public void setReality_type(String reality_type) {
            this.reality_type = reality_type;
        }

        public String getReality_name() {
            return reality_name;
        }

        public void setReality_name(String reality_name) {
            this.reality_name = reality_name;
        }

        public String getPage_id() {
            return page_id;
        }

        public void setPage_id(String page_id) {
            this.page_id = page_id;
        }

        public String getExposure_times() {
            return exposure_times;
        }

        public void setExposure_times(String exposure_times) {
            this.exposure_times = exposure_times;
        }

        public String getClick_times() {
            return click_times;
        }

        public void setClick_times(String click_times) {
            this.click_times = click_times;
        }

        public String getRes_ctr() {
            return res_ctr;
        }

        public void setRes_ctr(String res_ctr) {
            this.res_ctr = res_ctr;
        }

        public String getPlateform() {
            return plateform;
        }

        public void setPlateform(String plateform) {
            this.plateform = plateform;
        }

        public String getDevice_id() {
            return device_id;
        }

        public void setDevice_id(String device_id) {
            this.device_id = device_id;
        }

        public String getBegintime() {
            return begintime;
        }

        public void setBegintime(String begintime) {
            this.begintime = begintime;
        }

        public String getFavorite_times() {
            return favorite_times;
        }

        public void setFavorite_times(String favorite_times) {
            this.favorite_times = favorite_times;
        }

        public String getPraise_times() {
            return praise_times;
        }

        public void setPraise_times(String praise_times) {
            this.praise_times = praise_times;
        }

        public String getReply_times() {
            return reply_times;
        }

        public void setReply_times(String reply_times) {
            this.reply_times = reply_times;
        }

        public String getContext() {
            return context;
        }

        public void setContext(String context) {
            this.context = context;
        }

        @Override
        public String toString() {
            return "Ctr{" +
                    "reality_id='" + reality_id + '\'' +
                    ", reality_type='" + reality_type + '\'' +
                    ", reality_name='" + reality_name + '\'' +
                    ", page_id='" + page_id + '\'' +
                    ", exposure_times='" + exposure_times + '\'' +
                    ", click_times='" + click_times + '\'' +
                    ", res_ctr='" + res_ctr + '\'' +
                    ", plateform='" + plateform + '\'' +
                    ", device_id='" + device_id + '\'' +
                    ", begintime='" + begintime + '\'' +
                    ", favorite_times='" + favorite_times + '\'' +
                    ", praise_times='" + praise_times + '\'' +
                    ", reply_times='" + reply_times + '\'' +
                    ", context='" + context + '\'' +
                    '}';
        }
    }

}
