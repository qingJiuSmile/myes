package com.qingjiu.myes.entity.es;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * es配置实体
 * @author tjy
 * @date 2020/6/27
 **/
@Data
@ConfigurationProperties(prefix = "elasticsearch.security")
public class ElasticSearchEntity {

    @ApiModelProperty(value = "账号")
    private String userName;

    @ApiModelProperty(value = "密码")
    private String password;

    @ApiModelProperty(value = "路径")
    private String host;

    @ApiModelProperty(value = "端口")
    private Integer port;
}
