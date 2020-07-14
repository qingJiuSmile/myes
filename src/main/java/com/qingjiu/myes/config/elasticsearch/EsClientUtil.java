package com.qingjiu.myes.config.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.qingjiu.myes.entity.es.EsData;
import com.qingjiu.myes.entity.es.EsReturnData;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
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
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * ElasticSearch [Java-Rest-Client-High-Level] Util
 *
 * @author tjy
 * @date 2020/7/13
 **/
@Slf4j
@Component
public class EsClientUtil {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;


    /***********************************************************************************************
     ***                                 操 作 索 引 方 法                                        ***
     ***********************************************************************************************
     *                                date     :   2020-7-13 16:43:00                              *
     *                                author  :   tjy                                              *
     * ------------------------------------------------------------------------------------------- *
     * Methods:                                                                                    *
     *   createIndex                                                                               *
     *   openIndex                                                                                 *
     *   closeIndex                                                                                *
     *   deleteIndex                                                                               *
     *   existsIndex                                                                               *
     *   updateIndexSettings                                                                       *
     * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */


    /**
     * 创建索引
     *
     * @param indexName 索引名称 （索引名称必须全部小写）
     * @return boolean 是否成功
     * @author tjy
     * @date 2020/7/13
     **/
    public boolean createIndex(String indexName) throws IOException {

        // File jsonFile = ResourceUtils.getFile("classpath:name3.json");
        // 从json文件中获得配置
        // String json = FileUtils.readFileToString(jsonFile, "UTF-8");
        // 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        // 设置索引mapping类型映射
        // request.mapping(json, XContentType.JSON);
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        // 是否在超时之前为索引中的每个碎片启动了所需数量的碎片副本。
        // boolean shardsAcknowledged = response.isShardsAcknowledged();
        // 已确认请求。
        return response.isAcknowledged();
    }


    /**
     * 关闭索引
     *
     * @param indexName 索引名称
     * @return boolean 是否成功
     * @author tjy
     * @date 2020/7/13
     **/
    public boolean closeIndex(String indexName) throws IOException {
        CloseIndexRequest request = new CloseIndexRequest(indexName);
        AcknowledgedResponse closeIndexResponse = client.indices().close(request, RequestOptions.DEFAULT);
        return closeIndexResponse.isAcknowledged();
    }

    /**
     * 开启索引
     *
     * @param indexName 索引名称
     * @return boolean 是否成功
     * @author tjy
     * @date 2020/7/13
     **/
    public boolean openIndex(String indexName) throws IOException {
        OpenIndexRequest request = new OpenIndexRequest(indexName);
        OpenIndexResponse response = client.indices().open(request, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }


    /**
     * 修改索引配置(之前必须进行索引的 开关 操作否则报错)
     *
     * @param indexName 索引名称
     * @param map       参数
     * @return boolean 是否成功
     * @author tjy
     * @date 2020/7/13
     **/
    public boolean updateIndexSettings(String indexName, Map<String, Object> map) throws IOException {
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
        request.settings(map);
        AcknowledgedResponse updateSettingsResponse =
                client.indices().putSettings(request, RequestOptions.DEFAULT);
        return updateSettingsResponse.isAcknowledged();
    }

    /**
     * 删除索引 （慎用）
     *
     * @param indexName 索引名称
     * @return boolean  是否成功
     * @author tjy
     * @date 2020/7/13
     **/
    public boolean deleteIndex(String indexName) throws IOException {
        try {
            // 创建删除索引请求
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
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
     * @param indexName 索引名称（多个逗号分隔）
     * @return boolean  返回一个/多个索引是否存在（都存在返回true否则false）
     * @author tjy
     * @date 2020/7/13
     **/
    public boolean existsIndex(String... indexName) throws IOException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    /***********************************************************************************************
     ***                                 操 作 文 档 方 法                                        ***
     ***********************************************************************************************
     *                                date     :   2020-7-13 16:49:17                              *
     *                                author  :   tjy                                              *
     * ------------------------------------------------------------------------------------------- *
     * Methods:                                                                                    *
     *      addDocument                                                                            *
     *      getDocumentToMap                                                                       *
     *      getDocumentToString                                                                    *
     *      existsDocument                                                                         *
     *      deleteDocument                                                                         *
     *      updateDocument                                                                         *
     * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */


    /**
     * 添加文档（单个操作）
     *
     * @param indexName 索引名称
     * @param sourceObj 要存储的文档资源对象
     * @param id        添加文档id（不填默认为系统默认生成id）
     * @return 操作结果
     * @author tjy
     * @date 2020/7/13
     **/
    public String addDocument(String indexName, Object sourceObj, String id) throws IOException {

        IndexRequest request = new IndexRequest(indexName);
        if (!StringUtils.isEmpty(id)) {
            request.id(id);
        }
        try {
            request.source(JSON.toJSONString(sourceObj), XContentType.JSON);
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                log.info("[{}] ==> 添加成功 ", indexResponse);
            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                log.info("[{}] ==> 更新成功 ", indexResponse);
            }
            return indexResponse.status().toString();
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                // 版本冲突
                log.error(e.getMessage(), e);
            }
        }
        return null;
    }

    /**
     * 根据文档id 获得对应数据（map）
     *
     * @param indexName 索引id
     * @param id        doc id
     * @author tjy
     * @date 2020/7/13
     **/
    public Map<String, Object> getDocumentToMap(String indexName, String id) throws IOException {
        try {
            GetRequest request = new GetRequest(indexName, id);
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            // 多种形式返回（map）
            return response.getSourceAsMap();

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
     * 根据文档id 获得对应数据(json)
     *
     * @param indexName 索引名称
     * @param id        doc id
     * @return 是否成功 ok 为成功
     * @author tjy
     * @date 2020/7/13
     **/
    public String getDocumentToString(String indexName, String id) throws IOException {
        try {
            GetRequest request = new GetRequest(indexName, id);
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            // 多种形式返回（String）
            return response.getSourceAsString();

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
     * @return boolean
     * @author tjy
     * @date 2020/7/13
     **/
    public boolean existsDocument(String indexName, String id) throws IOException {
        GetRequest request = new GetRequest(indexName, id);
        return client.exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除对应id的文档
     *
     * @param indexName 索引名称
     * @param id        doc id
     * @return 是否成功 ok 为成功
     * @author tjy
     * @date 2020/7/13
     **/
    public String deleteDocument(String indexName, String id) throws IOException {

        try {
            DeleteRequest request = new DeleteRequest(indexName, id);
            DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
            // 找不到该文件
            if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                log.info("=== * 找不到该文档 * ===");
                return response.status().toString();
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

    /**
     * 修改对应id 文档内容
     *
     * @param indexName 索引名称
     * @param id        索引id
     * @param obj       要改变的对象
     * @return 是否成功 ok 为成功
     * @author tjy
     * @date 2020/7/13
     **/
    public String updateDocument(String indexName, String id, Object obj) throws IOException {

        try {
            UpdateRequest request = new UpdateRequest(indexName, id);
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
            return response.status().toString();
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
        return null;
    }

    /***********************************************************************************************
     ***                                 批   量   操   作                                       ***
     ***********************************************************************************************
     *                                date     :   2020-7-13 17:08:59                             *
     *                                author  :   tjy                                              *
     * ------------------------------------------------------------------------------------------- *
     * Method:                                                                                     *
     *   bulkAddDocument --                                                                        *
     *   bulkUpdateDocument --                                                                     *
     *   bulkDelDocument --                                                                        *
     * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */


    /**
     * 批量添加文档内容
     *
     * @param indexName 所有名称 （索引名称必须全部小写）
     * @param list      要添加的内容集合
     * @return boolean
     * @author tjy
     * @date 2020/7/13
     **/
    public boolean bulkAddDocument(String indexName, List<?> list) throws IOException {
        BulkRequest request = new BulkRequest();
        for (int i = 0; i < list.size(); i++) {

            request.add(new IndexRequest(indexName)
                    .source(JSON.toJSONString(list.get(i)), XContentType.JSON));
        }
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        // 是否失败返回false 代表成功
        return !bulkResponse.hasFailures();
    }


    /**
     * 批量修改
     *
     * @param indexName
     * @param list
     * @return boolean
     * @author tjy
     * @date 2020/7/13
     **/
    //TODO 这里批量修改需要索引id 不好入参 如果后续需要用到此方法，建议将此方法内容写入业务内自己根据参数循环。
 /*   public boolean bulkUpdateDocument(String indexName, List<?> list) throws IOException {

        BulkRequest request = new BulkRequest();
        for (Object aList : list) {

            request.add(new UpdateRequest(indexName, "").doc(JSON.toJSONString(aList), XContentType.JSON));
        }
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        // 是否失败返回false 代表成功
        return !bulkResponse.hasFailures();

    }*/

    /**
     * 批量删除文档
     *
     * @param indexName 文档名称
     * @param list      id 集合
     * @return boolean
     * @author tjy
     * @date 2020/7/13
     **/
    public boolean bulkDelDocument(String indexName, List<String> list) throws IOException {
        BulkRequest request = new BulkRequest();
        for (String aList : list) {
            request.add(new DeleteRequest(indexName, aList));
        }
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        // 是否失败返回false 代表成功
        return !bulkResponse.hasFailures();
    }


    /***********************************************************************************************
     ***                                 搜             索                                       ***
     ***********************************************************************************************
     *                                date     :   2020-7-13 17:29:41                              *
     *                                author  :   tjy                                              *
     * ------------------------------------------------------------------------------------------- *
     * Methods:                                                                                    *
     *      getDocByIds                                                                            *
     *      termQuery                                                                              *
     *      termsQuery                                                                             *
     *      matchQuery                                                                             *
     *      matchAllQuery                                                                          *
     *      multiMatchQuery                                                                        *
     *      matchOperatorQuery                                                                     *
     *      rangeFormQuery                                                                         *
     *      rangeNumQuery                                                                            *
     * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

    /**
     * TODO 排序功能没有SQL中那么灵活 目前先不用 因为使用不当就会影响结果
     * 排序公共方法
     *
     * @param sort    排序字段
     * @param builder
     * @return void
     * @author tjy
     * @date 2020/7/14
     **/
    private void buildSort(String sort, SearchSourceBuilder builder) {
        if (!StringUtils.isEmpty(sort)) {
            String[] arrSort = sort.split(",");
            if (arrSort.length == 2) {
                if ("asc".equals(arrSort[1]) || "ASC".equals(arrSort[1])) {
                    builder.sort(arrSort[0], SortOrder.ASC);
                    //  builder.sort(new FieldSortBuilder(arrSort[0]).order(SortOrder.ASC));
                } else if ("desc".equals(arrSort[1]) || "DESC".equals(arrSort[1])) {
                    builder.sort(arrSort[0], SortOrder.DESC);
                    // builder.sort(new FieldSortBuilder(arrSort[0]).order(SortOrder.DESC));

                }
            }
        }
    }

    /**
     * 根据多个文档id查询，类似Mysql中的where id in（1，2 ....）
     *
     * @param docIds    多个文档id
     * @param form      从第几个数据开始分页
     * @param size      从第几个数据结束分页
     * @param indexName 多个索引名称 （不填为全部）
     * @param sort      排序（预留）
     * @return com.weds.uipdorm.entity.es.EsReturnData
     * @author tjy
     * @date 2020/7/14
     **/
    public EsReturnData getDocByIds(Integer form, Integer size, String sort, String[] docIds,
                                    String... indexName) throws IOException {

        // 创建searchRequest
        SearchRequest request = new SearchRequest(indexName);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // 指定多个id进行查询
        builder.query(QueryBuilders.idsQuery().addIds(docIds));

        /*
         ******** 分页 ********
         */
        if (form != null && form > 0) {
            builder.from(form);
        }
        if (size != null && size > 0) {
            builder.size(size);
        }
        /*
         ******** 排序 ********
         */
        // buildSort(sort, builder);
        request.source(builder);
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        // 检索searchist
        SearchHits hits = searchResponse.getHits();
        TotalHits totalHits = hits.getTotalHits();
        // 将数据封装
        EsReturnData esReturnData = new EsReturnData();
        // 搜索出来的数据下的最大评分
        esReturnData.setMaxScore(hits.getMaxScore());
        // 数量统计
        esReturnData.setNum(totalHits.value);
        for (SearchHit hit : hits.getHits()) {
            EsData data = new EsData(hit.getSourceAsMap(), hit.getSourceAsString(), hit.getIndex(),
                    hit.getScore(), hit.getId());
            esReturnData.getEsDataList().add(data);
        }
        return esReturnData;
    }

    /**
     * 精准查询 （一个字段只能等于一个词）
     *
     * @param form      从第几个数据开始分页
     * @param size      从第几个数据结束分页
     * @param sort      排序（预留）
     * @param docName   文档字段名称
     * @param value     搜索值
     * @param indexName 多个索引名称 （不填为全部）
     * @return com.weds.uipdorm.entity.es.EsReturnData
     * @author tjy
     * @date 2020/7/14
     **/
    public EsReturnData termQuery(Integer form, Integer size, String sort,
                                  String docName, String value, String... indexName) throws IOException {

        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        /*
            termQuery 方法对中文支持不好，只能支持单个中文进行搜索；并且，如果是搜索单词的话
             也只能支持单个单词，如：不能 elasticSearch 驼峰写法
         */
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(docName + ".keyword", value);
        searchSourceBuilder.query(termQueryBuilder);

        /*
         ******** 分页 ********
         */
        if (form != null && form > 0) {
            searchSourceBuilder.from(form);
        }
        if (size != null && size > 0) {
            searchSourceBuilder.size(size);
        }
        /*
         ******** 排序 ********
         */
        // buildSort(sort, searchSourceBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 检索searchist
        SearchHits hits = response.getHits();
        TotalHits totalHits = hits.getTotalHits();
        // 将数据封装
        EsReturnData esReturnData = new EsReturnData();
        // 搜索出来的数据下的最大评分
        esReturnData.setMaxScore(hits.getMaxScore());
        // 数量统计
        esReturnData.setNum(totalHits.value);
        for (SearchHit hit : hits.getHits()) {
            EsData data = new EsData(hit.getSourceAsMap(), hit.getSourceAsString(), hit.getIndex(),
                    hit.getScore(), hit.getId());
            esReturnData.getEsDataList().add(data);
        }
        return esReturnData;
    }

    /**
     * 精准查询 （一个字段可以等于多个词） 类似于 SQL的 in(1,2,3)
     *
     * @param form      从第几个数据开始分页
     * @param size      从第几个数据结束分页
     * @param sort      排序（预留）
     * @param docName   文档字段名称
     * @param value     搜索值(一个或多个)
     * @param indexName 多个索引名称 （不填为全部）
     * @return com.weds.uipdorm.entity.es.EsReturnData
     * @author tjy
     * @date 2020/7/14
     **/
    public EsReturnData termsQuery(Integer form, Integer size, String sort,
                                   String docName, String[] value, String... indexName) throws IOException {

        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        /*
            termQuery 方法对中文支持不好，只能支持单个中文进行搜索；并且，如果是搜索单词的话
             也只能支持单个单词，如：不能 elasticSearch 驼峰写法
         */
        searchSourceBuilder.query(QueryBuilders.termsQuery(docName + ".keyword", value));

        /*
         ******** 分页 ********
         */
        if (form != null && form > 0) {
            searchSourceBuilder.from(form);
        }
        if (size != null && size > 0) {
            searchSourceBuilder.size(size);
        }
        /*
         ******** 排序 ********
         */
        // buildSort(sort, searchSourceBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 检索searchist
        SearchHits hits = response.getHits();
        TotalHits totalHits = hits.getTotalHits();
        // 将数据封装
        EsReturnData esReturnData = new EsReturnData();
        // 搜索出来的数据下的最大评分
        esReturnData.setMaxScore(hits.getMaxScore());
        // 数量统计
        esReturnData.setNum(totalHits.value);
        for (SearchHit hit : hits.getHits()) {
            EsData data = new EsData(hit.getSourceAsMap(), hit.getSourceAsString(), hit.getIndex(),
                    hit.getScore(), hit.getId());
            esReturnData.getEsDataList().add(data);
        }
        return esReturnData;
    }


    /**
     * 查询索引下所有文档的数据（注意 因为是搜索全部 所有评分都是1.0）
     *
     * @param form      从第几个数据开始分页
     * @param size      从第几个数据结束分页
     * @param sort      排序（预留）
     * @param indexName 多个索引名称 （不填为全部）
     * @return com.weds.uipdorm.entity.es.EsReturnData
     * @author tjy
     * @date 2020/7/14
     **/
    public EsReturnData matchAllQuery(Integer form, Integer size, String sort, String... indexName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        /*
         ******** 分页 ********
         */
        if (form != null && form > 0) {
            searchSourceBuilder.from(form);
        }
        if (size != null && size > 0) {
            searchSourceBuilder.size(size);
        }
        /*
         ******** 排序 ********
         */
        // buildSort(sort, searchSourceBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 检索searchist
        SearchHits hits = response.getHits();
        TotalHits totalHits = hits.getTotalHits();
        // 将数据封装
        EsReturnData esReturnData = new EsReturnData();
        // 搜索出来的数据下的最大评分
        esReturnData.setMaxScore(hits.getMaxScore());
        // 数量统计
        esReturnData.setNum(totalHits.value);
        for (SearchHit hit : hits.getHits()) {
            EsData data = new EsData(hit.getSourceAsMap(), hit.getSourceAsString(), hit.getIndex(),
                    hit.getScore(), hit.getId());
            esReturnData.getEsDataList().add(data);
        }
        return esReturnData;
    }


    /**
     * 查询索引下文档数据 多个value值空格分隔，并根据 AND/OR 规则对所有value的值进行分词检索
     *
     * @param form      从第几个数据开始分页
     * @param size      从第几个数据结束分页
     * @param sort      排序（预留）
     * @param docName   文档字段名称
     * @param operator  每个分词的关系 （AND/OR）
     * @param value     搜索值(使用空格隔开每一个要搜索的value分词)
     * @param indexName 多个索引名称 （不填为全部）
     * @return com.weds.uipdorm.entity.es.EsReturnData
     * @author tjy
     * @date 2020/7/14
     **/
    public EsReturnData matchOperatorQuery(Integer form, Integer size, String sort, String docName,
                                           Operator operator, Object value, String... indexName) throws IOException {

        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(docName, value).operator(operator));
        /*
         ******** 分页 ********
         */
        if (form != null && form > 0) {
            searchSourceBuilder.from(form);
        }
        if (size != null && size > 0) {
            searchSourceBuilder.size(size);
        }
        /*
         ******** 排序 ********
         */
        // buildSort(sort, searchSourceBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 检索searchist
        SearchHits hits = response.getHits();
        TotalHits totalHits = hits.getTotalHits();
        // 将数据封装
        EsReturnData esReturnData = new EsReturnData();
        // 搜索出来的数据下的最大评分
        esReturnData.setMaxScore(hits.getMaxScore());
        // 数量统计
        esReturnData.setNum(totalHits.value);
        for (SearchHit hit : hits.getHits()) {
            EsData data = new EsData(hit.getSourceAsMap(), hit.getSourceAsString(), hit.getIndex(),
                    hit.getScore(), hit.getId());
            esReturnData.getEsDataList().add(data);
        }
        return esReturnData;
    }


    /**
     * 查询索引下文档数据（分词查询） 类似于模糊匹配
     *
     * @param form      从第几个数据开始分页
     * @param size      从第几个数据结束分页
     * @param sort      排序（预留）
     * @param indexName 多个索引名称 （不填为全部）
     * @param docName   文档字段名称
     * @param value     搜索值
     * @return com.weds.uipdorm.entity.es.EsReturnData
     * @author tjy
     * @date 2020/7/14
     **/
    public EsReturnData matchQuery(Integer form, Integer size, String sort, String docName,
                                   Object value, String... indexName) throws IOException {

        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(docName, value));
        /*
         ******** 分页 ********
         */
        if (form != null && form > 0) {
            searchSourceBuilder.from(form);
        }
        if (size != null && size > 0) {
            searchSourceBuilder.size(size);
        }
        /*
         ******** 排序 ********
         */
        // buildSort(sort, searchSourceBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 检索searchist
        SearchHits hits = response.getHits();
        TotalHits totalHits = hits.getTotalHits();
        // 将数据封装
        EsReturnData esReturnData = new EsReturnData();
        // 搜索出来的数据下的最大评分
        esReturnData.setMaxScore(hits.getMaxScore());
        // 数量统计
        esReturnData.setNum(totalHits.value);
        for (SearchHit hit : hits.getHits()) {
            EsData data = new EsData(hit.getSourceAsMap(), hit.getSourceAsString(), hit.getIndex(),
                    hit.getScore(), hit.getId());
            esReturnData.getEsDataList().add(data);
        }
        return esReturnData;
    }

    /**
     * 查询索引下多个文档同一个value数据
     *
     * @param form      从第几个数据开始分页
     * @param size      从第几个数据结束分页
     * @param sort      排序（预留）
     * @param indexName 多个索引名称 （不填为全部）
     * @param docNames  文档名称（一个/多个）
     * @param value     搜索值
     * @return com.weds.uipdorm.entity.es.EsReturnData
     * @author tjy
     * @date 2020/7/14
     **/
    public EsReturnData multiMatchQuery(Integer form, Integer size, String sort, String[] docNames,
                                        Object value, String... indexName) throws IOException {

        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery(value, docNames));
        /*
         ******** 分页 ********
         */
        if (form != null && form > 0) {
            searchSourceBuilder.from(form);
        }
        if (size != null && size > 0) {
            searchSourceBuilder.size(size);
        }
        /*
         ******** 排序 ********
         */
        // buildSort(sort, searchSourceBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 检索searchist
        SearchHits hits = response.getHits();
        TotalHits totalHits = hits.getTotalHits();
        // 将数据封装
        EsReturnData esReturnData = new EsReturnData();
        // 搜索出来的数据下的最大评分
        esReturnData.setMaxScore(hits.getMaxScore());
        // 数量统计
        esReturnData.setNum(totalHits.value);
        for (SearchHit hit : hits.getHits()) {
            EsData data = new EsData(hit.getSourceAsMap(), hit.getSourceAsString(), hit.getIndex(),
                    hit.getScore(), hit.getId());
            esReturnData.getEsDataList().add(data);
        }
        return esReturnData;
    }


    /**
     * 区间范围搜索
     *
     * @param form      从第几个数据开始分页
     * @param size      从第几个数据结束分页
     * @param sort      排序（预留）
     * @param docName   文档字段名称
     * @param begin     区间字段 开始
     * @param end       区间字段 结束
     * @param indexName 多个索引名称 （不填为全部）
     * @return com.weds.uipdorm.entity.es.EsReturnData
     * @author tjy
     * @date 2020/7/14
     **/
    public EsReturnData rangeFormQuery(Integer form, Integer size, String sort, String docName, String begin,
                                       String end, String... indexName) throws IOException {
        // 创建请求
        SearchRequest request = new SearchRequest(indexName);
        // 时间范围的设定
        RangeQueryBuilder rangequerybuilder = QueryBuilders
                .rangeQuery(docName)
                .from(begin).to(end);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(rangequerybuilder);
        request.source(sourceBuilder);
        /*
         ******** 分页 ********
         */
        if (form != null && form > 0) {
            sourceBuilder.from(form);
        }
        if (size != null && size > 0) {
            sourceBuilder.size(size);
        }
        /*
         ******** 排序 ********
         */
        // buildSort(sort, searchSourceBuilder);
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 检索searchist
        SearchHits hits = response.getHits();
        TotalHits totalHits = hits.getTotalHits();
        // 将数据封装
        EsReturnData esReturnData = new EsReturnData();
        // 搜索出来的数据下的最大评分
        esReturnData.setMaxScore(hits.getMaxScore());
        // 数量统计
        esReturnData.setNum(totalHits.value);
        for (SearchHit hit : hits.getHits()) {
            EsData data = new EsData(hit.getSourceAsMap(), hit.getSourceAsString(), hit.getIndex(),
                    hit.getScore(), hit.getId());
            esReturnData.getEsDataList().add(data);
        }
        return esReturnData;
    }

    /**
     * 数值区间查询
     * @param form      从第几个数据开始分页
     * @param size      从第几个数据结束分页
     * @param sort      排序（预留）
     * @param docName   文档字段名称
     * @param lt        小于某值
     * @param gt        大于某值
     * @param isLte     是否小于等于
     * @param isGte     是否大于等于
     * @param indexName 多个索引名称 （不填为全部）
     * @return com.weds.uipdorm.entity.es.EsReturnData
     * @author tjy
     * @date 2020/7/14
     **/
    public EsReturnData rangeNumQuery(Integer form, Integer size, String sort, String docName, String lt,
                                    String gt, boolean isLte, boolean isGte, String... indexName) throws IOException {
        // 创建请求
        SearchRequest request = new SearchRequest(indexName);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        if (isLte && isGte) {
            builder.query(QueryBuilders.rangeQuery(docName).lte(lt).gte(gt));
        } else if (isGte) {
            builder.query(QueryBuilders.rangeQuery(docName).lt(lt).gte(gt));
        } else if (isLte) {
            builder.query(QueryBuilders.rangeQuery(docName).lte(lt).gt(gt));
        } else {
            builder.query(QueryBuilders.rangeQuery(docName).lt(lt).gt(gt));
        }

        /*
         ******** 分页 ********
         */
        if (form != null && form > 0) {
            builder.from(form);
        }
        if (size != null && size > 0) {
            builder.size(size);
        }
        /*
         ******** 排序 ********
         */
        // buildSort(sort, searchSourceBuilder);
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 检索searchist
        SearchHits hits = response.getHits();
        TotalHits totalHits = hits.getTotalHits();
        // 将数据封装
        EsReturnData esReturnData = new EsReturnData();
        // 搜索出来的数据下的最大评分
        esReturnData.setMaxScore(hits.getMaxScore());
        // 数量统计
        esReturnData.setNum(totalHits.value);
        for (SearchHit hit : hits.getHits()) {
            EsData data = new EsData(hit.getSourceAsMap(), hit.getSourceAsString(), hit.getIndex(),
                    hit.getScore(), hit.getId());
            esReturnData.getEsDataList().add(data);
        }
        return esReturnData;
    }

    /***********************************************************************************************
     ***                                 模     板     操   作                                    ***
     ***********************************************************************************************
     *                                date     :   2020-7-14 17:08:59                             *
     *                                author  :   tjy                                              *
     * ------------------------------------------------------------------------------------------- *
     * Method:                                                                                     *
     *                                                                                             *
     * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

    public void addIndexTemplate(){
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("my-template");
        request.patterns(Arrays.asList("pattern-1", "log-*"));
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1)
        );
    }
}
