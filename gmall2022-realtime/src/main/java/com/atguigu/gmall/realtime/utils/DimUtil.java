package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;


/**
 * 1.旁路缓存
 * 增： 如果不存在key，那么将key存入redis  ，同时设置过期时间
 * 更： 如果存在key，更新过期时间
 * 删： 如果数据更新了，那么删除
 */

public class DimUtil {


    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {


        //查询phoenix前先查询redis
        Jedis jedis = RedisUtil.getJedis();
        String key = "DIM:" + tableName + ":" + id;
        String jsonStr = jedis.get(key);

        if (jsonStr != null) {

            jedis.expire(key, 24 * 60 * 60);

            JSONObject data = JSON.parseObject(jsonStr);
//            System.out.println("走缓存>>>>>>" +data);

            jedis.close();

            return data;

        }
        //不在redis缓存中，去查询Phoenix
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + id + "'";
//        System.out.println(querySql);

        List<JSONObject> queryList = PhoenixUtil.queryList(connection, querySql, JSONObject.class, true);

        JSONObject result = queryList.get(0);
//        System.out.println("没走缓存>>>>>>>>>>>>>"+ result);

        //返回结果前，  将数据缓存至redis
        jedis.set(key, result.toJSONString());
        jedis.expire(key, 24 * 60 * 60);

        jedis.close();

        return result;

    }

    public static void delRedisDimInfo(String tableName, String id) {

        String key = "DIM:" + tableName + ":" + id;
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(key);
        jedis.close();

    }


    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        long start = System.currentTimeMillis();
        getDimInfo(connection, "DIM_SKU_INFO", "1");
        long end = System.currentTimeMillis();
        getDimInfo(connection, "DIM_SKU_INFO", "1");
        long end2 = System.currentTimeMillis();
        getDimInfo(connection, "DIM_SKU_INFO", "1");
        long end3 = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println(end2 - end);
        System.out.println(end3- end2);

    }

}
