package com.borjafpa.elasticsearch.bulk;

public enum BulkMetadata {
    INDEX("_index"),
    TYPE("_type"),
    ID("_id"),
    RETRY_ON_CONFLICT("_retry_on_conflict");
    
    String label;
    
    BulkMetadata(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }
}
