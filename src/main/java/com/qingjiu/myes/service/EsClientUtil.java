package com.qingjiu.myes.service;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
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
    private RestHighLevelClient restHighLevelClient;

    private long timeOut = 1;
    private long masterTimeOut = 2;


    public boolean createIndex(@NotNull String indexName, Long timeOut,
                               Long masterTimeOut, boolean isAsync) throws IOException {
        // 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

        // 超时时间
        request.setTimeout(TimeValue.timeValueMinutes(timeOut == null ? this.timeOut : timeOut));
        // 主节点超时时间
        request.setMasterTimeout(TimeValue.timeValueMinutes(masterTimeOut == null ? this.masterTimeOut : masterTimeOut));

        // 异步添加索引 todo 这里异步会抛异常，但是可以成功添加
        if (isAsync) {
            restHighLevelClient.indices().createAsync(request, RequestOptions.DEFAULT, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    log.info("createIndexResponse ==> [{}]", createIndexResponse.isAcknowledged());
                }

                @Override
                public void onFailure(Exception e) {
                    log.error(e.getMessage(),e);
                }
            });
        }

        // 已确认请求。
        boolean acknowledged = response.isAcknowledged();

        // 是否在超时之前为索引中的每个碎片启动了所需数量的碎片副本。
        // boolean shardsAcknowledged = response.isShardsAcknowledged();

        return acknowledged;
    }
}
