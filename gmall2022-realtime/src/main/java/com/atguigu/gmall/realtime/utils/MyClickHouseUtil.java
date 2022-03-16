package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql) {
        return  JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        try {

                            int offset = 0;
                            //获取所有字段
                            Field[] fields = t.getClass().getDeclaredFields();

                            //遍历字段
                            for (int i = 0; i < fields.length; i++) {

                                Field field = fields[i];

                                //设置私有属性可访问
                                field.setAccessible(true);
                                //获取字段上注解
                                TransientSink annotation = field.getAnnotation(TransientSink.class);

                                if (annotation != null) {
                                    //注解不为空
                                    offset++;
                                    continue;
                                }

                                //获取值
                                Object value = field.get(t);

//                                System.out.println("field>>>>"+field + "         value>>>>>"+value );

                                //给预编译SQL对象赋值
                                preparedStatement.setObject(i+1-offset,value);

                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }


                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()

        );
    }

}
