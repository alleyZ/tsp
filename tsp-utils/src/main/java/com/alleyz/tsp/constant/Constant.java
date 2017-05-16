package com.alleyz.tsp.constant;

/**
 * Created by alleyz on 2017/5/16.
 * 常量类
 */
public class Constant {
    private Constant(){}
    public final static String TXT_MSG_TOPIC = "origin-txt";

    public final static String TXT_MSG_GROUP_TOPOLOGY = "jstorm-topology";

    /**
     * 消息区块分割符
     */
    public final static String DELIMITER_BLOCK = "&";
    /**
     * 消息字段分割符
     */
    public final static String DELIMITER_FIELDS = "_";

    /**
     * z消费时的偏移位置 有三种取值 latest earliest none
     */
    public final static String OFFSET_RESET = "auto.offset.reset";

    /**
     * 消费时的偏移位置
     */
    public enum OffsetReset{
        LATEST("latest"), EARLIEST("earliest"), NONE("none");
        public String val;
        OffsetReset(String val) {
            this.val = val;
        }
    }
}
