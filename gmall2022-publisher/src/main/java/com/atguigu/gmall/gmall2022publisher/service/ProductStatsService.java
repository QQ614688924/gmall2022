package com.atguigu.gmall.gmall2022publisher.service;


import com.atguigu.gmall.gmall2022publisher.bean.ProductStats;
import com.atguigu.gmall.gmall2022publisher.bean.ProvinceStats;

import java.math.BigDecimal;
import java.util.List;

/**
 * Desc: 商品统计接口
 */
public interface ProductStatsService {
    //获取某一天的总交易额
    public BigDecimal getGMV(int date);

    //统计某天不同SPU商品交易额排名
    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit);

    //统计某天不同类别商品交易额排名
    public List<ProductStats> getProductStatsGroupByCategory3(int date,int limit);

    //统计某天不同品牌商品交易额排名
    public List<ProductStats> getProductStatsByTrademark(int date, int limit);



}
