package com.alleyz.tsp.store;

import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.constant.Constant;
import com.alleyz.tsp.kafka.consumer.SimpleConsumer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

import static com.alleyz.tsp.constant.Constant.DELIMITER_BLOCK;

/**
 * Created by alleyz on 2017/5/27.
 *
 */
public class HdfsConsumer implements Runnable{
    private static Logger logger = LoggerFactory.getLogger(HdfsConsumer.class);
    private SimpleConsumer consumer;
    private String oriPath;
    private Configuration hadoopConf;
    public HdfsConsumer(String topic, String group, String oriPath) {
        this.consumer = new SimpleConsumer(topic, group,
                ConfigUtils.getProp("/consumer.properties"), ConfigUtils.getLongVal("poll.interval.mills", 1000L));
        this.oriPath = oriPath;
        System.setProperty("HADOOP_USER_NAME", ConfigUtils.getStrVal("hadoop.user"));
        this.hadoopConf = new Configuration();
    }

    public void run() {
        while (true) {
            this.consumer.pollAndProcessMsg(crs -> {
                Map<String, List<String>> listMap = new HashMap<>();
                crs.forEach(cr -> {
                    String rowKey = cr.key();
                    String value = cr.value();
                    String prov = rowKey.substring(10, 12);
                    String month = StringUtils.reverse(rowKey.substring(0, 10)).substring(0, 6);
                    List<String> list = listMap.get(prov + "-" + month);
                    if (list == null) {
                        list = new ArrayList<>();
                        listMap.put(prov + "-" + month, list);
                    }
                    list.add(rowKey + DELIMITER_BLOCK + value);
                });
                try {
                    logger.info("rec msg batch ->" + listMap.size());
                    toHdfs(listMap);
                    return true;
                }catch (IOException e) {
                    return false;
                }
            });
        }
    }

    private void toHdfs(Map<String, List<String>> listMap) throws IOException{
        Iterator<Map.Entry<String, List<String>>> iterator = listMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<String>> entry = iterator.next();
            String rowKey = entry.getKey();
            String[] pm = rowKey.split("-");
            String prov = pm[0], month = pm[1];
            String hdfsPath = this.oriPath.replace("{prov}", prov).replace("{month}", month);
            Path path = new Path(hdfsPath);
            try(FileSystem fs = path.getFileSystem(this.hadoopConf)) {
                FSDataOutputStream fos;
                if(fs.exists(path)) {
                    fos = fs.append(path);
                }else{
                    fos = fs.create(path, false);
                }
                for(String content : entry.getValue()) {
                    ByteArrayInputStream bis = new ByteArrayInputStream(
                            (rowKey + Constant.DELIMITER_BLOCK + content + "\r\n").getBytes()
                    );
                    IOUtils.copyBytes(bis, fos, 4096, false);
                }
//                fos.hflush();
                fos.close();
            }
        }
//        listMap.forEach((k, lm) -> {
//            String prov = k.split("-")[0];
//            String month = k.split("-")[1];
//            Path path = new Path(this.oriPath.replace("{prov}", prov).replace("{month}", month));
//            try(FileSystem fs = path.getFileSystem(this.hadoopConf)){
//                FSDataOutputStream fos;
//                if(fs.exists(path)) {
//                    fos = fs.append(path);
//                }else{
//                    fos = fs.create(path);
//                }
//
//            }catch (IOException e) {
//
//            }
//        });
    }
}
