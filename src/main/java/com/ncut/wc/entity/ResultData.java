package com.ncut.wc.entity;

public class ResultData {
    public Long mmsi;
    public String windowStart;
    public double latMin;
    public double latMax;
    public double lonMin;
    public double lonMax;

    public ResultData() {

    }

    public ResultData(Long mmsi, String windowStart, double latMin, double latMax, double lonMin, double lonMax) {
        this.mmsi = mmsi;
        this.windowStart = windowStart;
        this.latMin = latMin;
        this.latMax = latMax;
        this.lonMin = lonMin;
        this.lonMax = lonMax;
    }

    @Override
    public String toString() {
        return "ResultData{" +
                "mmsi=" + mmsi +
                ", windowStart='" + windowStart + '\'' +
                ", latMin=" + latMin +
                ", latMax=" + latMax +
                ", lonMin=" + lonMin +
                ", lonMax=" + lonMax +
                '}';
    }

}
