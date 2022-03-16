package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {

//    private static Connection connection ;

//    private static Connection init() {
//        Connection connection = null;
//        try {
//            Class.forName(GmallConfig.PHOENIX_DRIVER);
//            connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//            connection.setSchema(GmallConfig.HBASE_SCHEMA);
//
//
//        } catch (Exception e) {
//            System.out.println("初始化连接失败");
//        }
//
//        return connection;
//    }

    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, Boolean underScoreToCamel) throws Exception {

//        if (connection == null){
//            connection = init();
//        }

        //编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        //执行查询语句
        ResultSet resultSet = preparedStatement.executeQuery();

        //获取元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        ArrayList<T> list = new ArrayList<>();

        while (resultSet.next()) {

            //获取类的实例
            T t = clz.newInstance();

            for (int i = 1; i < columnCount + 1; i++) {
                String columnName = metaData.getColumnName(i);

                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                Object value = resultSet.getObject(i);
                BeanUtils.setProperty(t, columnName, value);
            }

            //将每个实例添加到list中
            list.add(t);

        }
        preparedStatement.close();

        resultSet.close();

        return list;
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        String sql = "select * from GMALL2022_REALTIME.DIM_BASE_PROVINCE";

        System.out.println(PhoenixUtil.queryList(connection, sql, JSONObject.class, Boolean.TRUE));

    }


}
