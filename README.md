# elasticsearch-reindex
Java implementation for ElasticSearch Scroll/Bulk API

# How to use it?
- Add the project as a library

- Add the elasticsearch-reindex.properties to your project and set the elasticsearch host and port

- Import the follow classes

```java
import com.borjafpa.elasticsearch.util.AppConfiguration;
import com.borjafpa.elasticsearch.reindex.Reindex;

```

- In the code, add the path to get use the properties configured and then call the reindex method 
```java

AppConfiguration.setConfigurationFilePath(AppConfiguration.DEFAULT_FILE);

Map<String, Object> reindexParameters = new HashMap<String, Object>();
reindexParameters.put(Parameter.INDEX_ORIGIN.getKey(), "INDEX_ORIGIN");
reindexParameters.put(Parameter.INDEX_ORIGIN.getKey(), "TYPE_ORIGIN");
reindexParameters.put(Parameter.INDEX_ORIGIN.getKey(), "INDEX_DESTINATION");
reindexParameters.put(Parameter.INDEX_ORIGIN.getKey(), "TYPE_DESTINATION");
reindexParameters.put(Parameter.CREATE_INDEX_DESTINATION.getKey(), true/false);
reindexParameters.put(Parameter.MAPPING_DESTINATION.getKey(), "MAPPING");

Reindex.execute(reindexParameters);
```
