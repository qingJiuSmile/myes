package com.qingjiu.myes.service;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * ElasticSearch [Java-Rest-Client-High-Level] Util
 *
 * @author tjy
 * @date 2020/7/1
 **/
@Slf4j
@Component
public class EsClientUtil {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    private long timeOut = 1;
    private long masterTimeOut = 2;


    /**
     * 创建索引
     *
     * @param indexName     索引名称
     * @param timeOut       超时时间
     * @param masterTimeOut 主节点超时时间
     * @param isAsync       是否异步执行
     * @return boolean 是否成功
     * @author tjy
     * @date 2020/7/1
     **/
    public boolean createIndex(String indexName, Long timeOut, Long masterTimeOut, boolean isAsync) throws IOException {
        // 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        // 超时时间
        request.setTimeout(TimeValue.timeValueMinutes(timeOut == null ? this.timeOut : timeOut));
        // 主节点超时时间
        request.setMasterTimeout(TimeValue.timeValueMinutes(masterTimeOut == null ? this.masterTimeOut : masterTimeOut));
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);

        // 异步添加索引 todo 这里异步会抛异常，但是可以成功添加
        if (isAsync) {
            client.indices().createAsync(request, RequestOptions.DEFAULT, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    log.info("createIndexResponse ==> [{}]", createIndexResponse.isAcknowledged());
                }

                @Override
                public void onFailure(Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }

        // 已确认请求。
        boolean acknowledged = response.isAcknowledged();

        // 是否在超时之前为索引中的每个碎片启动了所需数量的碎片副本。
        // boolean shardsAcknowledged = response.isShardsAcknowledged();

        return acknowledged;
    }


    /**
     *  删除索引
     * @param indexName     索引名称
     * @param timeOut       超时时间
     * @param masterTimeOut 主节点超时时间
     * @param isAsync       是否异步执行
     * @author tjy
     * @date 2020/7/1
     * @return boolean 是否成功
     **/
    public boolean deleteIndex(String indexName, Long timeOut, Long masterTimeOut, boolean isAsync) throws IOException {

        try {
            // 创建删除索引请求
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            // 超时时间
            request.timeout(TimeValue.timeValueMinutes(timeOut == null ? this.timeOut : timeOut));
            // 主节点超时时间
            request.masterNodeTimeout(TimeValue.timeValueMinutes(masterTimeOut == null ? this.masterTimeOut : masterTimeOut));
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);

            if (isAsync) {

                client.indices().deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        log.info("AcknowledgedResponse ==> [{}]", acknowledgedResponse.isAcknowledged());

                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
            }

            return deleteIndexResponse.isAcknowledged();

        }catch (ElasticsearchException exception){
            if (exception.status() == RestStatus.NOT_FOUND) {
                 log.error("删除失败，找不到对应索引");
                 log.error(exception.getMessage(),exception);
            }else {
                log.error(exception.getMessage(),exception);
            }
        }
      return false;
    }
}
