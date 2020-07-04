package com.qingjiu.myes.util;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class Reptile {

    public static void main(String[] args) throws IOException {
        // 获取请求
        String url = "http://www.baidu.com";
        Document parse = Jsoup.parse(new URL(url), 30000);
    }

}
