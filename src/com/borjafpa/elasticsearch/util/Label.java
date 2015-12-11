package com.borjafpa.elasticsearch.util;

public enum Label {
    SCROLL_ID("_scroll_id"),
    HITS("hits"),
    SOURCE("_source"),
    NODES("nodes"),
    INDICES("indices"),
    SEARCH("search"),
    OPEN_CONTEXTS("open_contexts");
    
    String value;
    
    Label(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
