package com.atguigu.gmall.gmall2022publisher.service;

import com.atguigu.gmall.gmall2022publisher.bean.KeywordStats;

import java.util.List;
/**
 * Desc: 关键词统计接口
 */
public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}
