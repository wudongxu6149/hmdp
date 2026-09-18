package com.hmdp.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ScrollResult {
    private List<?> list;     // 博文集合
    private Long minTime;     // 本次查询的最小时间戳
    private Integer offset;   // 下一次查询的偏移量
}