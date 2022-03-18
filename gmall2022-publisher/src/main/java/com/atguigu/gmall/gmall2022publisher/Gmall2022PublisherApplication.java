package com.atguigu.gmall.gmall2022publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall.gmall2022publisher.mapper")
public class Gmall2022PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall2022PublisherApplication.class, args);
    }

}
