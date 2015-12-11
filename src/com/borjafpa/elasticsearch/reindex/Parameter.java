package com.borjafpa.elasticsearch.reindex;

public enum Parameter {
    CREATE_INDEX_DESTINATION("cid"),
    INDEX_ORIGIN("io"),
    INDEX_DESTINATION("id"),
    MAPPING_DESTINATION("md"),
    TYPE_ORIGIN("to"),
    TYPE_DESTINATION("td");
    
    String key;
    
    Parameter(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
