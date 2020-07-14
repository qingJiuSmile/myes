package com.qingjiu.myes.entity.es;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 *  ES 返回封装实体
 * @author tjy
 * @date 2020/7/13
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EsReturnData {

    @ApiModelProperty("返回数据数量")
    private Long num;

    @ApiModelProperty("当前数据最大分数")
    private Float maxScore;

    @ApiModelProperty("当前数据最大分数")
    private List<EsData> esDataList = new ArrayList<>();

}
