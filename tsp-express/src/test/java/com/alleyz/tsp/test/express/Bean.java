package com.alleyz.tsp.test.express;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by alleyz on 2017/5/18.
 */
public class Bean {
    private String tableName;
    private List<Map<String, Object>> propList = new ArrayList<>();

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Map<String, Object>> getPropList() {
        return propList;
    }

    public void setPropList(List<Map<String, Object>> propList) {
        this.propList = propList;
    }
}
