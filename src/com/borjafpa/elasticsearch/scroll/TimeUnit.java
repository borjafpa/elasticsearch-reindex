package com.borjafpa.elasticsearch.scroll;

public enum TimeUnit {
    YEAR("y"),
    MONTH("M"),
    WEEK("w"),
    DAY("d"),
    HOUR("h"),
    MINUTE("m"),
    SECOND("s");
    
    String value;
    
    TimeUnit(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
