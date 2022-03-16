package com.atguigu.gmall.hdfs;


//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class ReadCsvToHdfs {

    public static void main(String[] args) throws Exception {




        String localPath= "E:/logs/";
        String srcFile = "result2.csv";
        String localFile = localPath+ srcFile;

        //删除标题头
        removeLine(localFile);

//        String hdfsPath = "/mobile_id_mapping_md5/" + srcFile;
//        Configuration conf = new Configuration();
//
//        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "atguigu");
//        if(fs.exists(new Path(hdfsPath))){
//            System.out.println("存在" + hdfsPath);
//            fs.delete(new Path(hdfsPath));
//            System.out.println("删除该文件"+hdfsPath+"成功");
//        }
//        //上传本地文件
//        fs.copyFromLocalFile(new Path(localFile),new Path(hdfsPath));
//
//        fs.close();

    }

    public static void removeLine(String fileName) throws Exception {
        int lineDel = 1;
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        StringBuffer sb = new StringBuffer(4096);
        String temp = null;
        int line = 0;
        while ((temp = br.readLine()) != null) {
            line++;
            if (line == lineDel) continue;
            sb.append(temp).append("\r\n ");
        }
        br.close();
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
        bw.write(sb.toString());
        bw.close();

    }


}
