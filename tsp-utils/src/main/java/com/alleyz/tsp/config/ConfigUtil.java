package com.alleyz.tsp.config;

import lombok.extern.log4j.Log4j;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by alleyz on 2017/5/5.
 *
 */
@Log4j
public class ConfigUtil {
    private ConfigUtil (){}
    private static String fileName = "/application.properties";
    private static Properties prop = null;

    /**
     * 获取配置信息
     * @param configFiles 配置文件名称
     * @return 返回properties
     */
    public static Properties getProp(String configFiles){
        Properties prop = new Properties();
        try {
            prop.load(ConfigUtil.class.getResourceAsStream(configFiles));
        }catch (IOException e) {
            log.error("Can`t find config file [" +  configFiles + "]", e);
        }
        return prop;
    }

    /**
     * 获取配置字符串值
     * @param key key
     * @param defaults 默认值
     * @return 字符串
     */
    public static String getStrVal(String key, String defaults) {
        if(prop == null) prop = getProp(fileName);
        if(prop == null) return defaults;
        return prop.getProperty(key, defaults);
    }

    /**
     * 获取配置字符串值
     * @param key key
     * @return 值 如果没有则为null
     */
    public static String getStrVal(String key) {
        return getStrVal(key, null);
    }
    /**
     * 获取配置int值
     * @param key key
     * @param defaults 默认值
     * @return int
     */
    public static Integer getIntVal(String key, Integer defaults) {
        String val = getStrVal(key);
        if(val == null || "".equals(val)) return defaults;
        return Integer.parseInt(key);
    }

    public static Long getLongVal(String key, Long defaults) {
        String val = getStrVal(key);
        if(val == null || "".equals(val)) return defaults;
        return Long.parseLong(val);
    }
}
