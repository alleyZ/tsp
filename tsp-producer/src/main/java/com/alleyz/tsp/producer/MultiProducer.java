package com.alleyz.tsp.producer;

import com.alleyz.tsp.config.ConfigUtil;
import com.alleyz.tsp.kafka.producer.SimpleProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by alleyz on 2017/5/16.
 * 多线程生产消息
 *  读取指定目录下的文件
 */
public class MultiProducer {

    private static Logger log = LoggerFactory.getLogger(MultiProducer.class);

    private BlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);

    private Thread reader;

    private Thread[] workers;

    private volatile static Set<String> executed = ConcurrentHashMap.newKeySet();

    public MultiProducer(String path, String topic, int workerNum) {
        File pathFile = new File(path);
        reader = new Thread(() -> {
            while (true) {
                File[] files = pathFile.listFiles();
                if(files != null  && files.length > 0) {
                    for (File file : files) {
                        if(executed.contains(file.getName())) continue;
                        try(BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))){
                            String line = br.readLine();
                            queue.put(line);
                        }catch (IOException | InterruptedException e){
                            log.error("Producer-Reader", e);
                        }
                    }
                }
                try { Thread.sleep(2000L); }catch (Exception e) {e.printStackTrace();}
            }
        }, "Producer-Reader");
        Properties prop = ConfigUtil.getProp("/producer.properties");
        workers = new Thread[workerNum];
        for(int i = 0; i < workerNum; i++) {
            workers[i] = new Thread(()->{
                SimpleProducer producer = new SimpleProducer(topic, prop);
                while (true) {
                    try {
                        String line = queue.take();
                        producer.sendMsg(topic, line);
                    }catch (InterruptedException e) {
                        log.error(Thread.currentThread().getName(), e);
                    }
                }
            }, "Producer-Worker-" + i);
        }

    }


    public void start() {
        reader.start();
        log.info(reader.getName() + " is started!");
        for(Thread worker : workers) {
            worker.start();
            log.info(worker.getName() + " is started!");
        }
    }
}
