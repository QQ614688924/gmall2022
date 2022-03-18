package com.atguigu.gmall.gmall2022publisher.service;

import com.atguigu.gmall.gmall2022publisher.bean.ProvinceStats;

import java.util.List;
/**
 * Desc: 地区维度统计接口
 */
public interface ProvinceStatsService {

    public List<ProvinceStats> getProvinceStats(int date);
}
