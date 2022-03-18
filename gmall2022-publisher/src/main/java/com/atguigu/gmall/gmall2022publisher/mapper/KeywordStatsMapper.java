package com.atguigu.gmall.gmall2022publisher.mapper;


import com.atguigu.gmall.gmall2022publisher.bean.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import java.util.List;

/**
 * Desc: 关键词统计Mapper
 */
public interface KeywordStatsMapper {

    @Select("select keyword," +
            "sum(keyword_stats_2022.ct * " +
            "multiIf(source='SEARCH',10,source='ORDER',3,source='CART',2,source='CLICK',1,0)) ct" +
            " from keyword_stats_2022 where toYYYYMMDD(stt)=#{date} group by keyword " +
            "order by sum(keyword_stats_2022.ct) desc limit #{limit} ")
    public List<KeywordStats> selectKeywordStats(@Param("date") int date, @Param("limit") int limit);
}