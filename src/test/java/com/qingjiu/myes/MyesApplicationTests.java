package com.qingjiu.myes;

import com.alibaba.fastjson.JSON;
import com.qingjiu.myes.entity.es.ElasticSearchEntity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MyesApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private ElasticSearchEntity elasticSearchEntity;

    @Test
    public void contextLoads() {

        // 1、创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("nice_tjy");
        // 2、客户端执行请求 IndexClient
        try {
            CreateIndexResponse createIndexResponse =
                    restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

            System.out.println(createIndexResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testIndexExists() throws IOException {
        GetIndexRequest request = new GetIndexRequest("nice_tjy");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    public void testIndexDel() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("nice_tjy");
        AcknowledgedResponse exists = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(exists.isAcknowledged());
    }


    @Test
    public void testAddIndeex() throws IOException {

        IndexRequest request = new IndexRequest("test");
        request.id("3");
        IndexRequest source = request.source(JSON.toJSONString(elasticSearchEntity), XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        ActionListener<IndexResponse> actionListener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                System.out.println(1);
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println(2);
            }
        };
        restHighLevelClient.indexAsync(request, RequestOptions.DEFAULT, actionListener);
        System.out.println(source);
        System.out.println(index.toString());
        System.out.println(index.status());
    }


}
