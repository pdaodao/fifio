package com.github.apengda.fifio.odps.option;

import java.io.Serializable;

/**
 * odps 读相关配置参数
 */
public class OdpsReadOptions implements Serializable {
    // 分页获取数据行数
    private Long limit;
    // 读数据行数切分大小
    private Integer readSplitStepSize;
    // 查询
    private String queryTemplate;

    public Long getLimit() {
        return limit;
    }

    public OdpsReadOptions setLimit(Long limit) {
        this.limit = limit;
        return this;
    }

    public Integer getReadSplitStepSize() {
        return readSplitStepSize;
    }

    public OdpsReadOptions setReadSplitStepSize(Integer readSplitStepSize) {
        this.readSplitStepSize = readSplitStepSize;
        return this;
    }

    public String getQueryTemplate() {
        return queryTemplate;
    }

    public OdpsReadOptions setQueryTemplate(String queryTemplate) {
        this.queryTemplate = queryTemplate;
        return this;
    }
}
