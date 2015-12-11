package com.borjafpa.elasticsearch.scroll;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.borjafpa.elasticsearch.util.HttpUtil;
import com.borjafpa.elasticsearch.util.Label;
import com.borjafpa.elasticsearch.util.Parameter;

public class Scroll {
    private static final String ELASTIC_SEARCH_SCAN_SEARCH_TYPE = "search_type=scan"; 
    
    public static boolean areOpenContexts() {
        JSONObject contexts = getContextsAlive();
        
        JSONObject nodes = (JSONObject)contexts.get(Label.NODES.getValue());
        
        if ( nodes != null && !nodes.isEmpty() ) {
            Set<String> nodesSet = (Set<String>)nodes.keySet();
            for ( String nodeName: nodesSet ) {
                JSONObject node = (JSONObject)nodes.get(nodeName);
                JSONObject indices = (JSONObject)node.get(Label.INDICES.getValue());
                JSONObject search = (JSONObject)indices.get(Label.SEARCH.getValue());
                Long openContexts = (Long)search.get(Label.OPEN_CONTEXTS.getValue());
                
                if ( openContexts > 0 ) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    public static JSONObject getContextsAlive() {
        String contextsUrl = buildContextsUrl();
        HttpGet contextsRequest = new HttpGet(contextsUrl);
        
        return HttpUtil.executeGet(contextsRequest);
    }
    
    public static List<JSONObject> getResults(String indexName, String indexTypeName, TimeUnit timeUnit,
            Integer quantityTime, String scrollId) {
        
        List<JSONObject> results = new ArrayList<JSONObject>();
        
        String validScrollId = scrollId;
        
        if ( !isValidScrollId(scrollId) ) {
            String initialUrl = buildInitialUrl(indexName, indexTypeName, timeUnit, quantityTime);
            
            HttpGet initialRequest = new HttpGet(initialUrl);
            
            JSONObject initialJsonResult = HttpUtil.executeGet(initialRequest);
            
            if ( initialJsonResult != null ) {
                validScrollId = getScrollId(initialJsonResult);
            }
        }
        
        String consecutiveUrl = buildConsecutiveUrl(validScrollId, timeUnit, quantityTime);
        
        boolean moreResults = true;
        
        while ( moreResults ) {
            HttpGet consecutiveRequest = new HttpGet(consecutiveUrl);
            
            JSONObject consecutiveResult = HttpUtil.executeGet(consecutiveRequest);
            
            JSONArray documents = getDocuments(consecutiveResult);
            
            for ( int i = 0; i < documents.size(); i++ ) {
                JSONObject document = (JSONObject)documents.get(i);
                
                JSONObject source = (JSONObject)document.get(Label.SOURCE.getValue());
                results.add(source);
            }
            
            moreResults = documents.isEmpty();
        }
        
        return results;
    }
    
    public static JSONObject clearAllScroll() {
        String clearAllUrl = buildClearScrollUrl(null);
        HttpDelete clearAllRequest = new HttpDelete(clearAllUrl);
        
        return HttpUtil.executeDelete(clearAllRequest);
    }
    
    public static JSONObject clearScroll(String scrollId) {
        String clearAllUrl = buildClearScrollUrl(scrollId);
        HttpDelete clearAllRequest = new HttpDelete(clearAllUrl);
        
        return HttpUtil.executeDelete(clearAllRequest);
    }
    
    private static boolean isValidScrollId(String scrollId) {
        return scrollId != null && !scrollId.isEmpty();
    }
    
    private static String buildInitialUrl(String indexName, String indexTypeName, TimeUnit timeUnit,
            Integer quantityTime) {
        /*
         * curl -XGET 'localhost:9200/twitter/tweet/_search?scroll=15m&search_type=scan'
         */
        return Parameter.HTTP_UNSECURE + "://" 
                + Parameter.ELASTIC_SEARCH_HOST + ":" + Parameter.ELASTIC_SEARCH_PORT
                + "/" + indexName + "/" + indexTypeName + "/"
                + "_search?" + getScrollTimeUnit(timeUnit, quantityTime)
                + "&" + ELASTIC_SEARCH_SCAN_SEARCH_TYPE;
    }
    
    private static String buildConsecutiveUrl(String scrollId, TimeUnit timeUnit, Integer quantityTime) {
        /*
         * 'localhost:9200/_search/scroll?scroll=1m&scroll_id=SCROLL_ID'
         */
        return Parameter.HTTP_UNSECURE + "://"
                + Parameter.ELASTIC_SEARCH_HOST + ":" + Parameter.ELASTIC_SEARCH_PORT + "/"
                + "_search/scroll?" + getScrollTimeUnit(timeUnit, quantityTime)
                + "&scroll_id=" + scrollId;
    }
    
    private static String buildContextsUrl() {
        /*
         * curl -XGET localhost:9200/_nodes/stats/indices/search?pretty
         */
        
        return Parameter.HTTP_UNSECURE + "://"
                + Parameter.ELASTIC_SEARCH_HOST + ":" + Parameter.ELASTIC_SEARCH_PORT + "/"
                + "_nodes/stats/indices/search";
    }
    
    private static String buildClearScrollUrl(String scrollId) {
        /*
         * ALL: curl -XDELETE localhost:9200/_search/scroll/_all
         * WITH SCROLL ID: curl -XDELETE localhost:9200/_search/scroll?scroll_id=SCROLL_ID
         */
        
        String clearScrollUrl = Parameter.HTTP_UNSECURE + "://"
                + Parameter.ELASTIC_SEARCH_HOST + ":" + Parameter.ELASTIC_SEARCH_PORT + "/_search/scroll";
        
        if ( scrollId == null || scrollId.isEmpty() ) {
            clearScrollUrl += "/_all";
        } else {
            clearScrollUrl += "?scroll_id=" + scrollId;
        }
        
        return clearScrollUrl;
    }
    
    private static String getScrollTimeUnit(TimeUnit timeUnit, Integer quantityTime) {
        return "scroll=" + quantityTime + timeUnit.getValue(); 
    }
    
    private static String getScrollId(JSONObject jsonResponse) {
        
        if ( jsonResponse != null && !jsonResponse.isEmpty() ) {
            return (String)jsonResponse.get(Label.SCROLL_ID.getValue());
        }
        
        return null;
    }
    
    private static JSONArray getDocuments(JSONObject jsonResponse) {
        JSONArray documents = new JSONArray();
        
        if ( jsonResponse != null && !jsonResponse.isEmpty() ) {
            JSONObject hits = (JSONObject)jsonResponse.get(Label.HITS.getValue());
            
            if ( hits != null && !hits.isEmpty() ) {
                documents = (JSONArray)hits.get(Label.HITS.getValue());
            }
        }
        
        return documents;
    }
}
