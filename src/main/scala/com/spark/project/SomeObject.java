package com.spark.project;

public class SomeObject {
    private long[] rawdata;

    public void setFloatData(long[] dataFromPython) {
        // note that the dimensions are lost
        this.rawdata = dataFromPython;
    }

    public long[] getRawdata() {
        return rawdata;
    }

    public void setRawdata(long[] rawdata) {
        this.rawdata = rawdata;
    }
}
