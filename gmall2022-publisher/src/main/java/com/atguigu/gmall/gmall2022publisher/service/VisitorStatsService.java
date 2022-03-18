package com.atguigu.gmall.gmall2022publisher.service;

import com.atguigu.gmall.gmall2022publisher.bean.VisitorStats;

import java.util.List;

public interface VisitorStatsService {

    //新老访客流量统计
    public List<VisitorStats> getVisitorStatsByNewFlag(int date);

    //分时流量统计
    public List<VisitorStats> getVisitorStatsByHour(int date);

    public Long getPv(int date);

    public Long getUv(int date);

}
