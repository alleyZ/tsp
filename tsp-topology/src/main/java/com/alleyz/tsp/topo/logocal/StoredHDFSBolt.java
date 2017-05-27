package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alleyz.tsp.config.ConfigUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Map;

import static com.alleyz.tsp.constant.Constant.DELIMITER_BLOCK;
import static com.alleyz.tsp.constant.Constant.DELIMITER_FIELDS;
import static com.alleyz.tsp.topo.constant.TopoConstant.*;

/**
 * Created by alleyz on 2017/5/16.
 *
 * Deprecated hdfs不支持并发append，
 *  所以为提高整体吞吐量， 写文件bolt分离，单个写文件的的并行量 强制为1，否则会出错.
 *
 */
@Deprecated
public class StoredHDFSBolt implements IBasicBolt{
    public static final String NAME = "hdfs-bolt";
    private static Logger logger = LoggerFactory.getLogger(StoredHDFSBolt.class);
    private String oriPath;
    private String segPath;
    private String nwPath;
    private Configuration hadoopConf;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.hadoopConf = new Configuration();
        this.oriPath = ConfigUtils.getStrVal(PATH_HDFS_ORI);
        this.segPath = ConfigUtils.getStrVal(PATH_HDFS_SEG);
        this.nwPath = ConfigUtils.getStrVal(PATH_HDFS_NEW_WORD);
//        this.hadoopConf.set("fs.defaultFS", "hdfs://hd-29:8020");
        System.setProperty(HADOOP_USER_NAME, ConfigUtils.getStrVal(HADOOP_USER));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String realPath, content;
            switch (input.getSourceStreamId()) {
                case TOPOLOGY_STREAM_TXT_ID: {
                    String day = input.getStringByField(DEC_DAY),
                            prov = input.getStringByField(DEC_PROVINCE),
                            rowKey = input.getStringByField(DEC_ROW_KEY),
                            allTxt = input.getStringByField(DEC_ALL_TXT);
                    String month = day.substring(0, 6);
                    realPath = this.oriPath.replace("{prov}", prov).replace("{month}", month);
                    content = rowKey + DELIMITER_FIELDS + day + DELIMITER_BLOCK + allTxt + "\r\n";

                }
                break;
                case TOPOLOGY_STREAM_SEG_WORD_ID: {
                    String day = input.getStringByField(DEC_DAY),
                            prov = input.getStringByField(DEC_PROVINCE),
                            rowKey = input.getStringByField(DEC_ROW_KEY),
                            segTxt = input.getStringByField(DEC_SEG_WORD);
                    String month = day.substring(0, 6);
                    realPath = this.segPath.replace("{prov}", prov).replace("{month}", month);
                    content = rowKey + DELIMITER_FIELDS + day + DELIMITER_BLOCK + segTxt + "\r\n";
                }
                break;
                case TOPOLOGY_STREAM_NEW_WORD_ID:
                    String nw = input.getStringByField(DEC_NW_WORD);
                    realPath = this.nwPath;
                    content = "\r\n" + nw.replaceAll(" ", "\r\n");
                    break;
                default:
                    return;
            }
            System.out.println("hdfs-" + content);
            Path path = new Path(realPath);
            try(FileSystem fs = FileSystem.get(this.hadoopConf)){
                FSDataOutputStream fos;
                if (!fs.exists(path)) {
                    fos = fs.create(path);
                } else {
                    fos = fs.append(path);
                }
                ByteArrayInputStream is = new ByteArrayInputStream(content.getBytes());
                IOUtils.copyBytes(is, fos, 4096, true);
            }
        }catch (Exception e) {
            logger.error("yyyyyy", e);
            e.printStackTrace();
            collector.reportError(e);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
