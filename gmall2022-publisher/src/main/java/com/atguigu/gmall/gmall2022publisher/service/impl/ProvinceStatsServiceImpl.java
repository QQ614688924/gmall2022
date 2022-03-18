package com.atguigu.gmall.gmall2022publisher.service.impl;

import com.atguigu.gmall.gmall2022publisher.bean.ProvinceStats;
import com.atguigu.gmall.gmall2022publisher.mapper.ProvinceStatsMapper;
import com.atguigu.gmall.gmall2022publisher.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Desc:    按地区维度统计Service实现
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
