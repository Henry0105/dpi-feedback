package com.mob.dpi.utils;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import com.alibaba.fastjson.JSONObject;
import com.mob.dpi.pojo.ComparableList;
import com.mob.dpi.pojo.JavaBooleanObjectInspector;
import com.mob.dpi.pojo.JavaIntObjectInspector;
import com.mob.dpi.pojo.JavaStringObjectInspector;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * @Author: xuchl
 * @Date: 2022/3/2 10:37
 */
public class HttpUtils {
    private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

    public static String get(String url) {
        HttpResponse httpResponse =  HttpRequest.get(url)
                .header("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16")
                .execute();
        return httpResponse.body();
    }

    public static String sign(String secretKey, String data) throws Exception {
        SecretKeySpec signingKey = new SecretKeySpec(secretKey.getBytes(), HMAC_SHA1_ALGORITHM);
        Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
        mac.init(signingKey);
        byte[] rawHmac = mac.doFinal(data.getBytes());
        return org.apache.commons.codec.binary.Hex.encodeHexString(rawHmac);
    }


    /**
     * 获取指定日期的数据
     * @param date
     * @return
     * @throws Exception
     */
    public static List<ComparableList> getApiData(String date,String schema) throws Exception {
        // 接口方参数
        String url = "http://116.236.4.126:80";
        String username = "u_lx_datgrp6";
        String password = "u_lx_datgrp6@189";
        String apiKey = "OLsUbMEjr8of9av6x4PTzmOznXmYiBmk";

        String dbName = "u_lx_datgrp6";
        String tableName = "lx_info_nf_20";

        String[] columnAndTypes = schema.split(",");


        // 获取token
        long time = System.currentTimeMillis();
        String sign = sign(MD5Util.MD5(password), username + apiKey + time);
        String tokenUrl = url + "/getToken?apiKey=" + apiKey + "&sign=" + sign + "&timestamp=" + time;
        String tokenHtml = HttpUtils.get(tokenUrl);
        JSONObject tokenJson = JSONObject.parseObject(tokenHtml);
        String token = tokenJson.getString("result");

        // gw_id一样的有多条数据，不能去重
        List<ComparableList> rows = new ArrayList<ComparableList>();
        for (int i=0;i<1000000;i++){
            ComparableList<Object> row = new ComparableList<Object>();

            String getUrl = url + "/kv/get?token=" + token + "&database=" + dbName + "&table=" + tableName + "&key=job-eu_normal_" + date + "_".concat(String.valueOf(i));
            String getHtml = get(getUrl);
            JSONObject resultJson = JSONObject.parseObject(getHtml);

            Object result = resultJson.get("result");
            if(result != null){
                JSONObject json = JSONObject.parseObject(result.toString());
                String res = json.getString("value");
                System.out.println(res);

                String[] columns = res.split("\\|@\\|");
                for(int j=0;j<columns.length;j++){
                    String type = columnAndTypes[j].split(":")[1];
                    if("Boolean".equals(type)){
                        row.add(Boolean.valueOf(columns[j]));
                    } else if("Int".equals(type)) {
                        row.add(Integer.valueOf(columns[j].split("\\.")[0]));
                    } else {
                        row.add(columns[j]);
                    }
                }

                rows.add(row);
            } else {
                break;
            }
        }

        return rows;
    }


}
