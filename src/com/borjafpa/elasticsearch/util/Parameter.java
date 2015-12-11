package com.borjafpa.elasticsearch.util;

public class Parameter {
    public static final String HTTP_UNSECURE = "http";
    public static final String ELASTIC_SEARCH_HOST = AppConfiguration.get(AppConfigurationKey.HOST.name());
    public static final String ELASTIC_SEARCH_PORT = AppConfiguration.get(AppConfigurationKey.PORT.name());
}
