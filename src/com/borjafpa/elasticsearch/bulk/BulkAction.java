package com.borjafpa.elasticsearch.bulk;

public enum BulkAction {
    INDEX("index"),
    CREATE("create"),
    DELETE("delete"),
    UPDATE("update");
    
    String value;
    
    BulkAction(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
