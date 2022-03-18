package com.atguigu.gmall.realtime.utils;


import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    public static List<String> split_word(String keyword) throws IOException {
        ArrayList<String> resultList = new ArrayList<>();

        StringReader reader = new StringReader(keyword);

        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        while (true) {
            Lexeme next = ikSegmenter.next();
            if (next != null) {
                String word = next.getLexemeText();
                resultList.add(word);
            } else {
                break;
            }
        }
        return resultList;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(split_word("尚硅谷大数据之实时数仓是永远的神"));
    }

}
