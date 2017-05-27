package com.alleyz.tsp.producer;

import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.constant.Constant;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class Application{

    public static void main(String[] args) {
        MultiProducer producer = new MultiProducer(ConfigUtils.getStrVal("file.path"),
                Constant.TXT_MSG_TOPIC, ConfigUtils.getIntVal("worker.thread", 2));
        producer.start();
        while (true) {
            try {
                Thread.sleep(60000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
