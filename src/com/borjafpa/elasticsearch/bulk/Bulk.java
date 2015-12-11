package com.borjafpa.elasticsearch.bulk;

import java.util.List;
import java.util.Map;

import org.apache.http.client.methods.HttpPost;
import org.json.simple.JSONObject;

import com.borjafpa.elasticsearch.util.AppConfiguration;
import com.borjafpa.elasticsearch.util.AppConfigurationKey;
import com.borjafpa.elasticsearch.util.HttpUtil;
import com.borjafpa.elasticsearch.util.Parameter;

public class Bulk {
    
    public static JSONObject buildBulkActionAndMetadata(BulkAction bulkAction, Map<String, String> bulkMetadata) {
        JSONObject bulkActionAndMetadata = new JSONObject();
        JSONObject metadata = new JSONObject();
        
        for ( BulkMetadata bm: BulkMetadata.values() ) {
            if ( bulkMetadata.containsKey(bm.getLabel()) ) {
                metadata.put(bm.getLabel(), bulkMetadata.get(bm.getLabel()));
            }
        }
        
        bulkActionAndMetadata.put(bulkAction.getValue(), metadata);
        
        return bulkActionAndMetadata;
    }
    
    public static JSONObject execute(List<JSONObject> toBulk, Map<String, String> endpointsParameters) {
        String bulkToCreateUrl = buildBulkUrl(endpointsParameters);
        
        HttpPost bulkRequest = new HttpPost(bulkToCreateUrl);
        
        String parameters = buildBulkParameters(toBulk);
        
        return HttpUtil.executePost(bulkRequest, parameters);
    }
    
    private static String buildBulkParameters(List<JSONObject> bulkParameters) {
        String parameters = "";
        
        for ( JSONObject parameter: bulkParameters ) {
            parameters += parameter.toJSONString() + "\n";
        }
        
        parameters += "\n";
        
        return parameters;
    }
    
    private static String buildBulkUrl(Map<String, String> endpointsParameters) {
        String defaultIndex = endpointsParameters.get(BulkMetadata.INDEX.getLabel());
        String defaultType = endpointsParameters.get(BulkMetadata.TYPE.getLabel());
        
        String bulkUrl = Parameter.HTTP_UNSECURE + "://"
                + Parameter.ELASTIC_SEARCH_HOST + ":" + Parameter.ELASTIC_SEARCH_PORT + "/";
        
        if ( defaultIndex != null && !defaultIndex.isEmpty() ) {
            bulkUrl += defaultIndex + "/";
            
            if ( defaultType != null && !defaultType.isEmpty() ) {
                bulkUrl += defaultType + "/";
            }
        }
        
        bulkUrl += "_bulk";
        
        return bulkUrl; 
    }
}
