package com.qingjiu.myes.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
    private String userName;

    private String userNo;

    private Integer userId;

    private Integer age;

    private Integer sex;

    private String password;

    private String date;
}
