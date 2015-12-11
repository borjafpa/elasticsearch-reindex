package com.borjafpa.elasticsearch.index;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.json.simple.JSONObject;

import com.borjafpa.elasticsearch.util.HttpUtil;
import com.borjafpa.elasticsearch.util.Parameter;

public class Index {

    public static JSONObject create(String indexName) {
        String createIndexUrl = buildIndexCreationUrl(indexName);
        HttpGet createIndexRequest = new HttpGet(createIndexUrl);
        
        return HttpUtil.executeGet(createIndexRequest);
    }
    
    public static JSONObject createMapping(String indexName, String typeName, String mapping) {
        String createMappingUrl = buildMappingCreationUrl(indexName, typeName);
        
        HttpPost createMappingRequest = new HttpPost(createMappingUrl);
        
        return HttpUtil.executePost(createMappingRequest, mapping);
    }
    
    public static boolean isValidMapping(String mapping) {
        return mapping != null 
                && !mapping.trim().isEmpty() 
                && HttpUtil.parseResponse(mapping) != null;
    }

    private static String buildIndexCreationUrl(String indexName) {
        return Parameter.HTTP_UNSECURE + "://"
                + Parameter.ELASTIC_SEARCH_HOST + ":" + Parameter.ELASTIC_SEARCH_PORT + "/"
                + indexName + "/";
    }
    
    private static String buildMappingCreationUrl(String indexName, String typeName) {
        return Parameter.HTTP_UNSECURE + "://"
                + Parameter.ELASTIC_SEARCH_HOST + ":" + Parameter.ELASTIC_SEARCH_PORT + "/"
                + indexName + "/" + typeName + "/_mapping/";
    }
}
