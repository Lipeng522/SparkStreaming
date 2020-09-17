package com.atguigu.util;


import com.alibaba.fastjson.JSONObject;

//判定json字符串是否完整，如果完整则返回jsonobject，否则返回空值
public class ParseJsonData {
    public static JSONObject getJsonData(String data){
        try {

            return JSONObject.parseObject(data);
        }catch (Exception e){
            return null;
        }
    }
}
