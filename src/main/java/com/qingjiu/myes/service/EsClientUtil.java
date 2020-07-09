package com.qingjiu.myes.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qingjiu.myes.util.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.search.TotalHits;
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
     *                                date     :   2020-7-2 10:19:05                               *
     *                                author  :   tjy                                              *
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
        File jsonFile = ResourceUtils.getFile("classpath:name3.json");
        // 从json文件中获得配置
        String json = FileUtils.readFileToString(jsonFile, "UTF-8");

        // 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        // 超时时间
        request.setTimeout(TimeValue.timeValueSeconds(timeOut == null ? this.timeOut : timeOut));
        // 主节点超时时间
        request.setMasterTimeout(TimeValue.timeValueSeconds(masterTimeOut == null ? this.masterTimeOut : masterTimeOut));

        // 设置索引mapping类型映射
        request.mapping(json, XContentType.JSON);


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
     *                                date     :   2020-7-2 10:37:03                               *
     *                                author  :   tjy                                              *
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
            request.doc(JSON.toJSONString(obj), XContentType.JSON);
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
     *                                date     :   2020-7-3 15:34:23                               *
     *                                author  :   tjy                                              *
     * ------------------------------------------------------------------------------------------- *
     * Method:                                                                                     *
     *   bulkAddDocument --                                                                        *
     *   bulkUpdateDocument --                                                                     *
     *   bulkDelDocument --                                                                        *
     * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */


    public boolean bulkAddDocument(String indexName, List<?> list, Long timeOut, boolean isAsync) throws IOException {

        BulkRequest request = new BulkRequest();
        request.timeout(TimeValue.timeValueMinutes(timeOut == null ? this.timeOut : timeOut));
        for (int i = 0; i < list.size(); i++) {
            request.add(new IndexRequest(indexName).id("" + (i + 1))
                    .source(JSON.toJSONString(list.get(i)), XContentType.JSON));
        }

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

        if (isAsync) {
            client.bulkAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    log.info("bulkItemResponses ==> [{}]", bulkItemResponses.status());
                }

                @Override
                public void onFailure(Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
        // 是否失败返回false 代表成功
        return bulkResponse.hasFailures();
    }


    //TODO 批量修改;
    public boolean bulkUpdateDocument(String indexName, List<?> list, Long timeOut, boolean isAsync) throws IOException {

        BulkRequest request = new BulkRequest();
        request.timeout(TimeValue.timeValueMinutes(timeOut == null ? this.timeOut : timeOut));
        for (int i = 0; i < list.size(); i++) {
            request.add(new UpdateRequest(indexName, ("" + (i + 1))).doc(JSON.toJSONString(list.get(i)), XContentType.JSON));
        }

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

        if (isAsync) {
            client.bulkAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    log.info("bulkItemResponses ==> [{}]", bulkItemResponses.status());
                }

                @Override
                public void onFailure(Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
        // 是否失败返回false 代表成功
        return bulkResponse.hasFailures();

    }

    //TODO 批量删除;
    public boolean bulkDelDocument(String indexName, List<?> list, Long timeOut, boolean isAsync) throws IOException {
        BulkRequest request = new BulkRequest();
        request.timeout(TimeValue.timeValueMinutes(timeOut == null ? this.timeOut : timeOut));

        for (int i = 0; i < list.size(); i++) {
            request.add(new DeleteRequest(indexName, ("" + (i + 1))));
        }

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

        if (isAsync) {
            client.bulkAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    log.info("bulkItemResponses ==> [{}]", bulkItemResponses.status());
                }

                @Override
                public void onFailure(Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
        // 是否失败返回false 代表成功
        return bulkResponse.hasFailures();
    }


    /***********************************************************************************************
     ***                                 搜             索                                       ***
     ***********************************************************************************************
     *                                date     :   2020-7-3 15:47:35                               *
     *                                author  :   tjy                                              *
     * ------------------------------------------------------------------------------------------- *
     * Method:                                                                                     *
     *   bulkAddDocument --                                                                        *
     *   bulkUpdateDocument --                                                                     *
     *   bulkDelDocument --                                                                        *
     * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

    public void search() throws IOException {

        SearchRequest searchRequest = new SearchRequest();
        // 创建搜索构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 匹配所有
        // searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 搜索指定的索引
        // SearchRequest searchRequest1 = new SearchRequest("index1","index2");
        // 指定片区
        // searchRequest.preference("_local");

        // 查询所有的内容
        //searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        // b.查询包含关键词字段的文档：如下，表示查询出来所有包含user字段且user字段包含kimchy值的文档
        //searchSourceBuilder.query(QueryBuilders.termQuery("userName.keyword", "最强法海"));

        // 上面是基于QueryBuilders查询选项的，另外还可以使用MatchQueryBuilder配置查询参数
        QueryBuilder matchQueryBuilder = QueryBuilders.matchPhrasePrefixQuery("userName", "大");
        searchSourceBuilder.query(matchQueryBuilder);
        // 注：无论用于创建它的方法是什么，都必须将QueryBuilder对象添加到SearchSourceBuilder
      /*  TermsQueryBuilder termQueryBuilder = QueryBuilders.termsQuery("userName", "大威天龙");
        searchSourceBuilder.query(termQueryBuilder)*/
        ;

        // 设置查询的起始索引位置和数量：如下表示从第1条开始，共返回5条文档数据 （分页）
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(20);

        // 设置查询请求的超时时间：如下表示60秒没得到返回结果时就认为请求已超时
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //searchSourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.ASC));
        // 默认情况下，搜索请求会返回文档_source的内容，但与Rest API中的内容一样，您可以覆盖此行为。例如，您可以完全关闭_source检索：
        // searchSourceBuilder.fetchSource(false);

        // 该方法还接受一个或多个通配符模式的数组，以控制以更精细的方式包含或排除哪些字段
    /*    String[] includeFields = new String[] {"title", "user", "innerObject.*"};
        String[] excludeFields = new String[] {"_type"};
        SearchSourceBuilder searchSourceBuilder1 = searchSourceBuilder.fetchSource(includeFields, excludeFields);*/
        // System.out.println(searchSourceBuilder);

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("title");

        highlightTitle.highlighterType("unified");
        highlightBuilder.field(highlightTitle);

        HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
        highlightBuilder.field(highlightUser);

        searchSourceBuilder.highlighter(highlightBuilder);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // 检索searchist
        SearchHits hits = searchResponse.getHits();
        System.out.println(JSON.toJSONString(hits));
        TotalHits totalHits = hits.getTotalHits();
        // 数量统计
        long numHits = totalHits.value;
        TotalHits.Relation relation = totalHits.relation;
        float maxScore = hits.getMaxScore();
        System.out.println(numHits);
        System.out.println(relation);
        System.out.println(maxScore);

        for (SearchHit hit : hits.getHits()) {
            // do something with the SearchHit
            String index = hit.getIndex();
            String id = hit.getId();
            float score = hit.getScore();
            String sourceAsString = hit.getSourceAsString();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
        /*    String documentTitle = (String) sourceAsMap.get("title");
            List<Object> users = (List<Object>) sourceAsMap.get("user");
            Map<String, Object> innerObject =
                    (Map<String, Object>) sourceAsMap.get("innerObject");*/
            log.info("法师所在索引 || ==> [{}] || id ==> [{}] || 评分 ==> [{}] || 有那味了 ==> [{}]", index, id, score, sourceAsMap);
        }
       /* for (SearchHit hit : hits.getHits()) {
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlight = highlightFields.get("qs");
            Text[] fragments = highlight.fragments();
            String fragmentString = fragments[0].string();
            System.out.println(fragmentString);
        }*/
      /*  Aggregations aggregations = searchResponse.getAggregations();
        Terms byCompanyAggregation = aggregations.get("by_company");
        Terms.Bucket elastic = byCompanyAggregation.getBucketByKey("Elastic");
        Avg averageAge = elastic.getAggregations().get("average_age");
        double avg = averageAge.getValue();
        System.out.println(avg);*/

    }


    public void testTermQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // termQuery 方法对中文支持不好，只能支持单个中文进行搜索；并且，如果是搜索单词的话 也只能支持单个单词，如：不能 elasticSearch 驼峰写法
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("userName", "kingfahai");
        searchSourceBuilder.query(termQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
        System.out.println(JSON.toJSONString(response.getHits()));
        for (SearchHit searchHit : response.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsMap());
        }
    }


    public void searchMutil() throws IOException {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // QueryBuilder queryBuilder = QueryBuilders.termQuery("userName.keyword", "大");
        // QueryBuilders.termsQuery("user", new ArrayList<String>().add("kimchy"));
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("userName", "大");
        // 多个index 索引去匹配
        // QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery("法海", "userName","id", "userNo");
        //  QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        // MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("userName", "法海");
     /*   // 对匹配查询启用模糊匹配
        matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        // 在匹配查询上设置前缀长度
        matchQueryBuilder.prefixLength(3);
        // 设置最大展开选项以控制查询的模糊过程
        matchQueryBuilder.maxExpansions(10);*/

        // 排序，默认为全局倒序
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        //searchSourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC));
        searchSourceBuilder.query(queryBuilder);
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
        System.out.println(JSON.toJSONString(response.getHits()));
        for (SearchHit searchHit : response.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsMap());
        }
    }

    public void timeSearch() throws IOException {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("date", "2020-07-09");
        // 查询大于给定的时间
        // QueryBuilders.rangeQuery("date").gt(DateUtil.nowTimestamp());
        // 查询区间时间
        // QueryBuilders.rangeQuery("date").lt("2020-07-08").gt("2020-07-09 02:42:23");
        // 插叙小于给定的时间
        // QueryBuilders.rangeQuery("date").lt(DateUtil.nowTimestamp());
        searchSourceBuilder.query(queryBuilder);
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
        System.out.println(JSON.toJSONString(response.getHits()));
        for (SearchHit searchHit : response.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsMap());
        }
    }

}
