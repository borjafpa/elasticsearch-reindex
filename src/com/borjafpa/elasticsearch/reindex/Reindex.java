package com.borjafpa.elasticsearch.reindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import com.borjafpa.elasticsearch.bulk.Bulk;
import com.borjafpa.elasticsearch.bulk.BulkAction;
import com.borjafpa.elasticsearch.bulk.BulkMetadata;
import com.borjafpa.elasticsearch.index.Index;
import com.borjafpa.elasticsearch.scroll.Scroll;
import com.borjafpa.elasticsearch.scroll.TimeUnit;

public class Reindex {
    public static JSONObject execute(Map<String, Object> reindexParameters) {
        boolean createDestinationIndex = (Boolean) reindexParameters.get(Parameter.CREATE_INDEX_DESTINATION.getKey());
        
        if ( createDestinationIndex ) {
            String indexDestination = (String) reindexParameters.get(Parameter.INDEX_DESTINATION.getKey());
            String typeDestination = (String) reindexParameters.get(Parameter.TYPE_DESTINATION.getKey());
            String mappingDestination = (String) reindexParameters.get(Parameter.MAPPING_DESTINATION.getKey());
            
            Index.create(indexDestination);
            
            if ( Index.isValidMapping(mappingDestination) ) {
                Index.createMapping(indexDestination, typeDestination, mappingDestination);
            }
        }
        
        return transfer(reindexParameters);
    }
    
    private static JSONObject transfer(Map<String, Object> reindexParameters) {
        String indexOrigin = (String) reindexParameters.get(Parameter.INDEX_ORIGIN.getKey());
        String typeOrigin = (String) reindexParameters.get(Parameter.TYPE_ORIGIN.getKey());
        String indexDestination = (String) reindexParameters.get(Parameter.INDEX_DESTINATION.getKey());
        String typeDestination = (String) reindexParameters.get(Parameter.TYPE_DESTINATION.getKey());
        
        List<JSONObject> results = Scroll.getResults(indexOrigin, typeOrigin, TimeUnit.MINUTE, 10, null);
        
        List<JSONObject> toBulk = new ArrayList<JSONObject>();
        
        Map<String, String> bulkMetadata = new HashMap<String, String>();
        bulkMetadata.put(BulkMetadata.INDEX.getLabel(), indexDestination);
        bulkMetadata.put(BulkMetadata.TYPE.getLabel(), typeDestination);
        
        JSONObject actionAndMetadata = Bulk.buildBulkActionAndMetadata(BulkAction.CREATE, bulkMetadata);
        
        for ( JSONObject result: results ) {
            toBulk.add(actionAndMetadata);
            toBulk.add(result);
        }
        
        Map<String, String> endpointsParameters = new HashMap<String, String>();
        
        return Bulk.execute(toBulk, endpointsParameters);
    }
    
    public static void main(String[] args) {
        
        Map<String, Object> reindexParameters = new HashMap<String, Object>();
        reindexParameters.put(Parameter.INDEX_ORIGIN.getKey(), "twitter");
        reindexParameters.put(Parameter.INDEX_ORIGIN.getKey(), "tweet");
        reindexParameters.put(Parameter.INDEX_ORIGIN.getKey(), "microblog");
        reindexParameters.put(Parameter.INDEX_ORIGIN.getKey(), "post");
        
        execute(reindexParameters);
    }
}
