package com.borjafpa.elasticsearch.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class HttpUtil {
    private static Logger logger = Logger.getLogger(HttpUtil.class);
    
    public static JSONObject executePost(HttpPost postRequest, String parameters) {
        if ( postRequest != null && parameters != null ) {
            
            StringEntity inputJsonData;
            try {
                inputJsonData = new StringEntity(parameters);
                inputJsonData.setContentType("application/json");

                postRequest.setEntity(inputJsonData);
                postRequest.addHeader("accept", "application/json");
                postRequest.addHeader("Connection", "Keep-Alive");

                return execute(postRequest);
                
            } catch (UnsupportedEncodingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        return null;
    }
    
    public static JSONObject executeGet(HttpGet getRequest) {
        return execute(getRequest);
    }
    
    public static JSONObject executeDelete(HttpDelete deleteRequest) {
        return execute(deleteRequest);
    }
    
    private static JSONObject execute(HttpUriRequest request) {
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpResponse response = null;
        StringBuffer jsonString = null;
        
        try {
            
                response = httpClient.execute(request);
            
            if ( response.getStatusLine().getStatusCode() != HttpStatus.SC_OK 
                    && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED ) {
                logger.error("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
            }
            
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                            response.getEntity().getContent()
                            )
                    );

            String output = "";
            jsonString = new StringBuffer();

            while ( (output = br.readLine()) != null ) {
                jsonString.append(output);
            }
        } catch (ClientProtocolException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
        
        return parseResponse(jsonString.toString());
    }
    
//    public static JSONObject getResponse(HttpGet getRequest, HttpDelete deleteRequest) {
//        
//        HttpClient httpClient = HttpClientBuilder.create().build();
//        HttpResponse response = null;
//        StringBuffer jsonString = null;
//        
//        try {
//            
//            if ( getRequest != null ) {
//                response = httpClient.execute(getRequest);
//            } else if ( deleteRequest != null ) {
//                response = httpClient.execute(deleteRequest);
//            }
//            
//            if ( response.getStatusLine().getStatusCode() != HttpStatus.SC_OK 
//                    && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED ) {
//                logger.error("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
//            }
//            
//            BufferedReader br = new BufferedReader(
//                    new InputStreamReader(
//                            response.getEntity().getContent()
//                            )
//                    );
//
//            String output = "";
//            jsonString = new StringBuffer();
//
//            while ( (output = br.readLine()) != null ) {
//                jsonString.append(output);
//            }
//        } catch (ClientProtocolException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        } finally {
//            HttpClientUtils.closeQuietly(httpClient);
//        }
//        
//        return parseResponse(jsonString);
//    }
    
    public static JSONObject parseResponse(String jsonString) {
        JSONParser jsonParser = null;
        JSONObject jsonResponse = null;
        
        if ( jsonString != null ) {
            jsonParser = new JSONParser();
            try {
                jsonResponse = (JSONObject)jsonParser.parse(jsonString);
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } 
        }
        
        return jsonResponse;
    }
}
