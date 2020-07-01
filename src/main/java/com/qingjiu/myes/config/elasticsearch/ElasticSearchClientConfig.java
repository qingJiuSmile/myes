package com.qingjiu.myes.config.elasticsearch;


import com.qingjiu.myes.entity.es.ElasticSearchEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author lenovo
 */
@Configuration
public class ElasticSearchClientConfig {


    private final ElasticSearchEntity elasticSearchEntity;

    public ElasticSearchClientConfig(ElasticSearchEntity elasticSearchEntity) {
        this.elasticSearchEntity = elasticSearchEntity;
    }

    @Bean
    public RestHighLevelClient restHighLevelClient() {

        // 创建设置安全验证请求
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                // 设置账号 密码
                new UsernamePasswordCredentials(elasticSearchEntity.getUserName(), elasticSearchEntity.getPassword()));

        RestClientBuilder builder = RestClient.builder(new HttpHost(elasticSearchEntity.getHost(), elasticSearchEntity.getPort()))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }


}
