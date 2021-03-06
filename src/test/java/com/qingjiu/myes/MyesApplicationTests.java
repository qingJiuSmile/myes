package com.qingjiu.myes;

import com.alibaba.fastjson.JSON;
import com.qingjiu.myes.entity.User;
import com.qingjiu.myes.entity.es.ElasticSearchEntity;
import com.qingjiu.myes.service.EsClientUtil;
import com.qingjiu.myes.util.Base64Utils;
import com.qingjiu.myes.util.DateUtil;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

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
        // request.opType(DocWriteRequest.OpType.INDEX);
        //request.version(2);
        IndexRequest source = request.source(JSON.toJSONString(elasticSearchEntity), XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        if (index.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("添加成功");
            System.out.println("index:" + index);
        } else if (index.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功");
            System.out.println("index:" + index);
        }
        // 异步
      /*  ActionListener<IndexResponse> actionListener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                System.out.println(indexResponse.status());
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println(2);
            }
        };
        restHighLevelClient.indexAsync(request, RequestOptions.DEFAULT, actionListener);*/
        System.out.println(source);
        System.out.println(index.toString());
        System.out.println(index.status());
    }

    @Test
    public void testGetIndex() throws IOException {
        GetRequest getRequest = new GetRequest("test", "4");
        try {
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            if (getResponse.isExists()) {
                ElasticSearchEntity elasticSearchEntity = JSON.parseObject(getResponse.getSourceAsString(), ElasticSearchEntity.class);
                System.out.println(elasticSearchEntity);
            } else {
                System.out.println("document不存在");
            }
        } catch (ElasticsearchException e) {
            // index不存在时抛出
            if (e.status() == RestStatus.NOT_FOUND) {
                System.out.println("index不存在");
            }
            // 如果请求了特定的文档版本，而现有文档有不同的版本号，则会引发版本冲突：
            if (e.status() == RestStatus.CONFLICT) {
                System.out.println("版本冲突异常");
            }
        }
        // 异步
        /*restHighLevelClient.getAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists()) {
                    ElasticSearchEntity elasticSearchEntity = JSON.parseObject(getResponse.getSourceAsString(), ElasticSearchEntity.class);
                    System.out.println(elasticSearchEntity);
                } else {

                }

            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        });*/

    }


    @Test
    public void testGetSource() throws IOException {
        GetRequest request = new GetRequest("test", "1");
        boolean exists = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }


    @Test
    public void testDelIndex() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("test", "4");
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        ReplicationResponse.ShardInfo shardInfo = delete.getShardInfo();
        System.out.println(delete.status());
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            System.out.println("成功");
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure :
                    shardInfo.getFailures()) {
                String reason = failure.reason();
                System.out.println(reason);
            }
        }
    }

    @Test
    public void testUpdate() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("test", "4");
        ElasticSearchEntity user = new ElasticSearchEntity();
        user.setHost("123456");
        user.setPassword("654321");
        user.setPort(6379);
        user.setUserName("tjy");
        updateRequest.doc(JSON.toJSON(user), XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update.status());
    }


    /**
     * ****************************************************************************************************************
     */

    @Test
    public void test() throws IOException {
        // 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("name6");
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

        // 超时时间
        request.setTimeout(TimeValue.timeValueMinutes(2));
        // 主节点超时时间
        request.setMasterTimeout(TimeValue.timeValueMinutes(1));

        // 异步添加索引
      /*  restHighLevelClient.indices().createAsync(request,RequestOptions.DEFAULT, new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                System.out.println(createIndexResponse.isAcknowledged());
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        });*/

        // 已确认请求。
        boolean acknowledged = response.isAcknowledged();
        // 是否在超时之前为索引中的每个碎片启动了所需数量的碎片副本。
        boolean shardsAcknowledged = response.isShardsAcknowledged();
        System.out.println(acknowledged);
        System.out.println(shardsAcknowledged);
    }

    @Autowired
    private EsClientUtil esClientUtil;

    @Test
    public void testAddIndexUtil() throws IOException {
        boolean util = esClientUtil.createIndex("name3", null, null, false);
        System.out.println(util);
    }

    @Test
    public void testDelIndexUtil() throws IOException {
        boolean util = esClientUtil.deleteIndex("name3", null, null, false);
        System.out.println(util);
    }

    @Test
    public void testExitsIndexUtil() throws IOException {
        boolean b = esClientUtil.existsIndex(false, "name4", "name2");
        System.out.println(b);
    }


    @Test
    public void testAddDocUtil() throws IOException {
        User user = new User();
        user.setUserNo("2221111");
        user.setUserName("最强法海");
        user.setSex(1);
        RestStatus myes = esClientUtil.addDocument("myes", null, user, true, "2");
        System.out.println(myes);
    }

    @Test
    public void testGetDocToStingUtil() throws IOException {
        String documentToMap = esClientUtil.getDocumentToString("myes", "1", false);
        System.out.println(documentToMap);
    }

    @Test
    public void testGetDocToMapUtil() throws IOException {
        Map<String, Object> documentToMap = esClientUtil.getDocumentToMap("myes", "2", true);
        System.out.println(documentToMap);
    }

    @Test
    public void testGetExistsUtil() throws IOException {
        boolean myes = esClientUtil.existsDocument("myes", "KeWGDXMBy0tykx2fj1nG", true);
        System.out.println(myes);
    }

    @Test
    public void testGetDelDocUtil() throws IOException {
        String myes = esClientUtil.deleteDocument("myes", "KOV_DXMBy0tykx2fCFn-", null, false);
        System.out.println(myes);
    }

    @Test
    public void testUpdateDocUtil() throws IOException {
        User user = new User();
        user.setUserNo("222233333");
        user.setUserName("大威天龙");
        user.setSex(1);
        esClientUtil.updateDocument("myes", "1", user, null, false);
    }

    @Test
    public void testAddBulkDocument() throws IOException {
        List<User> list = new ArrayList<>();

        String timestamp = DateUtil.timestampToStr(DateUtil.nowTimestamp(), null);
        //Date timestamp = new Date();

        System.out.println(timestamp);
        User user = new User();
        user.setUserNo("23");
        user.setUserName("kingfahai");
        user.setSex(1);
        user.setDate("2020-07-09 10:00:40");
        list.add(user);

        User user1 = new User();
        user1.setUserNo("2");
        user1.setUserName("daweitianlong ");
        user1.setSex(2);
        user1.setDate("2020-07-08 10:33:59");
        list.add(user1);


        User user2 = new User();
        user2.setUserNo("3");
        user2.setUserName("bore");
        user2.setSex(3);
        user2.setDate("2020-07-09 07:34:10");
        list.add(user2);

        User user3 = new User();
        user3.setUserNo("4");
        user3.setUserName("大罗金身1");
        user3.setSex(3);
        user3.setDate("2020-07-09 03:34:10");
        list.add(user3);

        User user4 = new User();
        user4.setUserNo("5");
        user4.setUserName(Base64Utils.ImageToBase64ByOnline("http://bpic.588ku.com//element_origin_min_pic/17/03/03/7bf4480888f35addcf2ce942701c728a.jpg"));
        user4.setSex(3);
        user4.setDate("2020-07-09 02:34:10");
        list.add(user4);

        User user5 = new User();
        user5.setUserNo("6");
        user5.setUserName("开始捉妖12");
        user5.setSex(3);
        user5.setDate("2020-07-09 05:34:10");
        list.add(user5);

        User user6 = new User();
        user6.setUserNo("7");
        user6.setUserName("牛皮!");
        user6.setSex(2);
        user6.setDate(timestamp);
        list.add(user6);

        boolean json = esClientUtil.bulkAddDocument("name3", list, null, false);
        // String json = esClientUtil.bulkUpdateDocument("myes", list, null, false);
        // String json = esClientUtil.bulkDelDocument("myes", list, null, false);
        System.out.println(json);
    }


    @Test
    public void testSearch() throws IOException {
        esClientUtil.timeSearch();
    }


    @Test
    public void updateIndexSetings() throws IOException {
        boolean name31 = esClientUtil.closeIndex("name3", false);
        System.out.println("关闭索引" + name31);
        Map<String, Object> map = new HashMap<>();
        // 索引一旦建立，主分片数量不可改变
       // String settingKey = "index.number_of_shards";
        String settingKey1 = "index.number_of_replicas";
       // map.put(settingKey, 2);
        map.put(settingKey1, 0);
        boolean name3 = esClientUtil.updateIndexSetings("name3", map, false);
        System.out.println(name3);
        boolean name32 = esClientUtil.openIndex("name3", false);
        System.out.println(name32 + "开启索引");
    }

    @Test
    public void  getTermsQuery() throws IOException {
        esClientUtil.termsQuery("name3","userName","大罗金身1","最强法海");
    }

}
