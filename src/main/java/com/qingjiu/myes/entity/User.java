package com.qingjiu.myes.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
}
