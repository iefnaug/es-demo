package com.es.nativeapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;

/**
 * @author GF
 * @since 2023/4/26
 */
@Slf4j
public class DemoTest {

    @Test
    public void connectTest() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200)));
//        index(client);
//        insert(client);
//        update(client);
//        batch(client);
        search(client);
        client.close();
    }

    private void index(RestHighLevelClient client) throws IOException {
        //创建索引
        CreateIndexRequest request = new CreateIndexRequest("user");
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }


    private void insert(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest();
        request.index("user");
        request.id("11");

        User user = new User();
        user.setId(11L);
        user.setName("afei");
        user.setAge(1111);

        ObjectMapper objectMapper = new ObjectMapper();
        String source = objectMapper.writeValueAsString(user);

        request.source(source, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }


    private void update(RestHighLevelClient client) throws IOException {
        UpdateRequest request = new UpdateRequest();
        request.index("user").id("11");

        User user = new User();
        user.setName("haha");

        ObjectMapper objectMapper = new ObjectMapper();
        String source = objectMapper.writeValueAsString(user);
        System.err.println(source);
        request.doc(source, XContentType.JSON);

        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

        System.err.println(response);

    }


    private void batch(RestHighLevelClient client) throws IOException {
        BulkRequest request = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest().index("user").create(false).id(String.valueOf(1001)).source(XContentType.JSON, "name", "scott");
        IndexRequest indexRequest2 = new IndexRequest().index("user").create(true).id(String.valueOf(1001)).source(XContentType.JSON, "name", "scott2");
        IndexRequest indexRequest3 = new IndexRequest().index("user").create(true).id(String.valueOf(1001)).source(XContentType.JSON, "name", "scott3");

        request.add(indexRequest);
        request.add(indexRequest2);
        request.add(indexRequest3);

        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        System.err.println(response);
    }


    private void search(RestHighLevelClient client) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        builder.from(0);
        builder.size(1);
        builder.sort("id", SortOrder.DESC);
//        builder.fetchSource()

        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        log.info("response: {}", response);
    }

    @Data
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    class User {
        Long id;
        String name;
        Integer age;
    }

}
