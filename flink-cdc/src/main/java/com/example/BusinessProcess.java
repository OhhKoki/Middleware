package com.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import java.io.Serializable;

public class BusinessProcess implements Serializable {
    /**
     * 每次数据同步过来之后，进入当前方法进行业务处理
     */
    public void processing(String dataStr) {
        // 1.将 String 转为 JSON
        JSONObject jsonObject = JSONObject.parseObject(dataStr);
        // 2.获取 tableName
        String tableName = (String) jsonObject.get("tableName");
        // 3.根据不同的 tableName 进行不同的业务处理操作
        businessTableProcessing(tableName);
    }
    /**
     * 不同的 table 进行不同的 business Process
     * @param tableName
     */
    private void businessTableProcessing(String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("table name is null");
        }else if ("stuinfo".equals(tableName)) {
            new StuInfoHandler().businessTableProcessing();
        }else if ("stuinfo_bar".equals(tableName)) {
            new StuInfoBarHandler().businessTableProcessing();
        }
    }
}