package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyKafkaUtil {


    private static String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    private static Properties properties  = new Properties();

    static {
        properties.setProperty("bootstrap.servers",KAFKA_SERVER);
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> serializationSchema){
        String defaultTopic = "dwd_base_db";


        return  new FlinkKafkaProducer<T>(defaultTopic,serializationSchema,properties,FlinkKafkaProducer.Semantic.NONE);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);
    }

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }

}
