package com.qingjiu.myes;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MyesApplication {

    public static void main(String[] args) {
        SpringApplication.run(MyesApplication.class, args);
        System.setProperty("es.set.netty.runtime.available.processors", "false");

    }

}
