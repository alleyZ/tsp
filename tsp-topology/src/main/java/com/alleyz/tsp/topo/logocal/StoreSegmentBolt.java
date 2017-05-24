package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alleyz.tsp.config.ConfigUtil;
import com.alleyz.tsp.topo.utils.TopologyHelper;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.alleyz.tsp.constant.Constant.DELIMITER_BLOCK;
import static com.alleyz.tsp.topo.constant.TopoConstant.*;

/**
 * Created by alleyz on 2017/5/22.
 *
 */
public class StoreSegmentBolt implements IBasicBolt {
    public static final String NAME = "store-segment-hdfs-bolt";
    private static Logger logger = LoggerFactory.getLogger(StoreSegmentBolt.class);
    private String segPath;
    private Configuration hadoopConf;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.hadoopConf = new Configuration();
        this.segPath = ConfigUtil.getStrVal(PATH_HDFS_SEG);
        System.setProperty(HADOOP_USER_NAME, ConfigUtil.getStrVal(HADOOP_USER));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TOPOLOGY_STREAM_SEG_WORD_ID.equals(input.getSourceStreamId())){
            String day = input.getStringByField(DEC_DAY),
                    prov = input.getStringByField(DEC_PROVINCE),
                    rowKey = input.getStringByField(DEC_ROW_KEY),
                    segTxt = input.getStringByField(DEC_SEG_WORD);
            String month = day.substring(0, 6);
            String realPath = this.segPath.replace("{prov}", prov).replace("{month}", month);
            String content = rowKey + DELIMITER_BLOCK + day + DELIMITER_BLOCK + segTxt + "\r\n";
            try {
                TopologyHelper.appendHDFS(realPath, content, this.hadoopConf);
            }catch (IOException e) {
                logger.error("store segment 2 hdfs has error", e);
            }
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
