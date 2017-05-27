package com.alleyz.tsp.store;

import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.constant.Constant;

/**
 * Created by alleyz on 2017/5/27.
 *
 */
public class Application {
    public static void main(String[] args) {
        new Thread( // 存储原始文本
                new HdfsConsumer(
                        Constant.TXT_MSG_TOPIC,
                        "hdfs-txt",
                        ConfigUtils.getStrVal("path.hdfs.origin")),
                "ori-kafka->hdfs")
                .start();

        new Thread( // 存储分词
                new HdfsConsumer(
                        Constant.SEG_MSG_TOPIC,
                        "hdfs-txt",
                        ConfigUtils.getStrVal("path.hdfs.senment")),
                "ori-kafka->hdfs")
                .start();
    }
}
