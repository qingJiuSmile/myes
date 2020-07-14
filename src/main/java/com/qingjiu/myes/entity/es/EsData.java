package com.qingjiu.myes.entity.es;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EsData {

    @ApiModelProperty("返回数据集合 map类型")
    private Map<String, Object> sourceAsMap;

    @ApiModelProperty("返回数据集合 json字符串")
    private String sourceAsString;

    @ApiModelProperty("所在索引")
    private String index;

    @ApiModelProperty("评分")
    private Float score;

    @ApiModelProperty("索引id")
    private String indexId;

}
