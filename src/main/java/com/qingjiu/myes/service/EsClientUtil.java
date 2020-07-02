package com.qingjiu.myes.service;

import com.alibaba.fastjson.JSON;
import com.sun.org.apache.regexp.internal.RE;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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

    /***********************************************************************************************
     ***                                 操 作 索 引 方 法                                        ***
     ***********************************************************************************************
     *                                date  :   2020-7-2 10:19:05                                  *
     *                                auth  :   tjy                                                *
     * ------------------------------------------------------------------------------------------- *
     * Methods:                                                                                    *
     *   createIndex --                                                                            *
     *   deleteIndex --                                                                            *
     *   existsIndex --                                                                            *
     * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

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
     * 删除索引
     *
     * @param indexName     索引名称
     * @param timeOut       超时时间
     * @param masterTimeOut 主节点超时时间
     * @param isAsync       是否异步执行
     * @return boolean 是否成功
     * @author tjy
     * @date 2020/7/1
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

        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {
                log.error("删除失败，找不到对应索引");
                log.error(exception.getMessage(), exception);
            } else {
                log.error(exception.getMessage(), exception);
            }
        }
        return false;
    }

    /**
     * 判断索引是否存在，多个索引逗号分隔
     *
     * @param isAsync   是否异步
     * @param indexName 索引名称（多个逗号分隔）
     * @return boolean  返回一个/多个索引是否存在（都存在返回true否则false）
     * @author tjy
     * @date 2020/7/2
     **/
    public boolean existsIndex(boolean isAsync, String... indexName) throws IOException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        if (isAsync) {
            client.indices().existsAsync(request, RequestOptions.DEFAULT, new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean exists) {
                    log.info("索引 [{}]  是否存在 ==> [{}]", indexName, exists);
                }

                @Override
                public void onFailure(Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
        return exists;
    }

    /***********************************************************************************************
     ***                                 操 作 文 档 方 法                                        ***
     ***********************************************************************************************
     *                                date  :   2020-7-2 10:37:03                                  *
     *                                auth  :   tjy                                                *
     * ------------------------------------------------------------------------------------------- *
     * Method:                                                                                     *
     *   addDocument -- Fetches the number of frames in data block.                                *
     *   Get_Build_Frame_Width -- Fetches the width of the shape image.                            *
     *   Get_Build_Frame_Height -- Fetches the height of the shape image.                          *
     * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */


    /**
     * 添加文档
     *
     * @param indexName 索引名称
     * @param timeOut   超时时间
     * @param sourceObj 要存储的文档资源对象
     * @param isAsync   是否异步
     * @param id        文档id
     * @return org.elasticsearch.rest.RestStatus 操作结果
     * @author tjy
     * @date 2020/7/2
     **/
    public RestStatus addDocument(String indexName, Long timeOut, Object sourceObj, boolean isAsync,
                                  String id) throws IOException {

        IndexRequest request = new IndexRequest(indexName);
        if (!StringUtils.isEmpty(id)) {
            request.id(id);
        }
        // request.opType(DocWriteRequest.OpType.INDEX);
        // request.version(2);
        request.timeout(TimeValue.timeValueSeconds(timeOut == null ? this.timeOut : timeOut));

        try {
            request.source(JSON.toJSONString(sourceObj), XContentType.JSON);
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                log.info("[{}] ==> 添加成功 ", indexResponse);
            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                log.info("[{}] ==> 更新成功 ", indexResponse);
            }
            return indexResponse.status();
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                // 版本冲突
                log.error(e.getMessage(), e);
            }
        }

        if (isAsync) {
            // 异步
            client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    log.info("indexResponse.status() ==> [{}]", indexResponse.status());
                }

                @Override
                public void onFailure(Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
        return null;
    }

    /**
     * 根据文档id 获得对应数据
     *
     * @param indexName 索引id
     * @param id        doc id
     * @param isAsync   是否异步
     * @author tjy
     * @date 2020/7/2
     **/
    public Map<String, Object> getDocumentToMap(String indexName, String id, boolean isAsync) throws IOException {
        try {
            GetRequest request = new GetRequest(indexName, id);
            GetResponse response = client.get(request, RequestOptions.DEFAULT);

            // 多种形式返回（map）
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            // 异步方法
            if (isAsync) {
                client.getAsync(request, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(GetResponse getResponse) {
                        // 异步方法无返回值，在这处理逻辑
                        log.info("返回数据 ==> [{}]", getResponse.getSourceAsMap());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
            }
            return sourceAsMap;

        } catch (ElasticsearchException e) {
            // 没有找到文档
            if (e.status() == RestStatus.NOT_FOUND) {
                log.error(e.getMessage(), e);
            }
            // 版本冲突
            if (e.status() == RestStatus.CONFLICT) {
                log.error(e.getMessage(), e);
            }
        }
        return null;
    }

    /**
     * 根据文档id 获得对应数据
     *
     * @param indexName 索引名称
     * @param id        doc id
     * @param isAsync   是否异步
     * @return java.lang.String
     * @author tjy
     * @date 2020/7/2
     **/
    public String getDocumentToString(String indexName, String id, boolean isAsync) throws IOException {
        try {
            GetRequest request = new GetRequest(indexName, id);
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            // 多种形式返回（String）
            String sourceAsMap = response.getSourceAsString();
            // 异步方法
            if (isAsync) {
                client.getAsync(request, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(GetResponse getResponse) {
                        // 异步方法无返回值，在这处理逻辑
                        log.info("返回数据 ==> [{}]", getResponse.getSourceAsMap());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
            }
            return sourceAsMap;

        } catch (ElasticsearchException e) {
            // 没有找到文档
            if (e.status() == RestStatus.NOT_FOUND) {
                log.error(e.getMessage(), e);
            }
            // 版本冲突
            if (e.status() == RestStatus.CONFLICT) {
                log.error(e.getMessage(), e);
            }
        }
        return null;
    }

    /**
     * 查询对应索引下的文档是否存在
     *
     * @param indexName 索引名称
     * @param id        文档id
     * @param isAsync   是否异步
     * @return boolean
     * @author tjy
     * @date 2020/7/2
     **/
    public boolean existsDocument(String indexName, String id, boolean isAsync) throws IOException {
        GetRequest request = new GetRequest(indexName, id);
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        if (isAsync) {
            client.existsAsync(request, RequestOptions.DEFAULT, new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean aBoolean) {
                    log.info("对应文档是否存在 ==> [{}]", aBoolean);
                }

                @Override
                public void onFailure(Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
        return exists;
    }

    /**
     * 删除对应id的doc
     *
     * @param indexName 索引名称
     * @param id        doc id
     * @param timeOut   超时时间
     * @param isAsync   是否异步
     * @return java.lang.String
     * @author tjy
     * @date 2020/7/2
     **/
    public String deleteDocument(String indexName, String id, Long timeOut, boolean isAsync) throws IOException {

        try {
            DeleteRequest request = new DeleteRequest(indexName, id);
            request.timeout(TimeValue.timeValueMinutes(timeOut == null ? this.timeOut : timeOut));
            DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
            // 找不到该文件
            if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                log.info("=== * 找不到该文档 * ===");
                return response.getResult().toString();
            }
            if (isAsync) {
                client.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
                    @Override
                    public void onResponse(DeleteResponse deleteResponse) {
                        log.info("删除doc [{}] ==> [{}]", deleteResponse.getIndex(), deleteResponse.status());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
            }
            return response.status().toString();
        } catch (ElasticsearchException e) {
            // 版本冲突
            if (e.status() == RestStatus.CONFLICT) {
                log.error(e.getMessage(), e);
            }
        }
        return null;
    }

    // TODO 没整明白
    public void updateDocument(String indexName, String id, Object obj, Long timeOut, boolean isAsync) throws IOException {

        try {
            UpdateRequest request = new UpdateRequest(indexName, id);
            request.timeout(TimeValue.timeValueSeconds(timeOut == null ? this.timeOut : timeOut));
            request.doc(JSON.toJSON(obj), XContentType.JSON);
            UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

            // 处理第一次创建文档的情况(向上插入)
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                log.info("处理第一次创建文档的情况 ==> [{}]", response.status());
                // 处理文档更新的情况
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                log.info("处理文档更新的情况 ==> [{}]", response.status());
                // 处理文档被删除的情况
            } else if (response.getResult() == DocWriteResponse.Result.DELETED) {
                log.info("处理文档被删除的情况 ==> [{}]", response.status());
                // 处理文档未受更新影响的情况(未对文档执行任何操作(Noop))。
            } else if (response.getResult() == DocWriteResponse.Result.NOOP) {
                log.info("处理文档未受更新影响的情况(未对文档执行任何操作(Noop)) ==> [{}]", response.status());
            }

            if (isAsync) {
                client.updateAsync(request, RequestOptions.DEFAULT, new ActionListener<UpdateResponse>() {
                    @Override
                    public void onResponse(UpdateResponse updateResponse) {
                        log.info("修改文档 ==> [{}]", updateResponse.status());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
            }

            log.info("[{}]", response.status());
        } catch (ElasticsearchException e) {
            // 文档不存在
            if (e.status() == RestStatus.NOT_FOUND) {
                log.error(e.getMessage(), e);
            }

            // 版本冲突
            if (e.status() == RestStatus.CONFLICT) {
                log.error(e.getMessage(), e);
            }
        }

    }

    /***********************************************************************************************
     ***                                 批   量   操   作                                       ***
     ***********************************************************************************************
     *                                date  :   2020-7-2 16:46:54                                  *
     *                                auth  :   tjy                                                *
     * ------------------------------------------------------------------------------------------- *
     * Method:                                                                                     *
     *   addDocument -- Fetches the number of frames in data block.                                *
     *   Get_Build_Frame_Width -- Fetches the width of the shape image.                            *
     *   Get_Build_Frame_Height -- Fetches the height of the shape image.                          *
     * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */


    public void addBulkDocument(String indexName, List<?> list, Long timeOut) throws IOException {
        BulkRequest request = new BulkRequest();
        for (int i = 0; i < list.size(); i++) {
            request.add(new IndexRequest(indexName).id("" + i)
                    .source(JSON.toJSON(list.get(i)), XContentType.JSON))
                    .timeout(TimeValue.timeValueMinutes(timeOut == null ? this.timeOut : timeOut));
        }

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.status());
    }

}
