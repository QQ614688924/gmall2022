package com.atguigu.gmall;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("applog")
    public String appLog(@RequestParam("param") String jsonStr){
        //接受日志数据
//        System.out.println(jsonStr);

        //将日志落盘
        log.info(jsonStr);

        //写入kafka中
        kafkaTemplate.send("ods_base_log",jsonStr);

        return "success";
    }


}
