package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
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
 */
public class StoreOriginBolt implements IBasicBolt {
    private static Logger logger = LoggerFactory.getLogger(StoreOriginBolt.class);
    public static final String NAME = "store_hdfs_origin_bolt";
    private String oriPath;
    private Configuration hadoopConf;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.hadoopConf = new Configuration();
        this.oriPath = ConfigUtil.getStrVal(PATH_HDFS_ORI);
        System.setProperty(HADOOP_USER_NAME, ConfigUtil.getStrVal(HADOOP_USER));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TOPOLOGY_STREAM_TXT_ID.equals(input.getSourceStreamId())){
            String day = input.getStringByField(DEC_DAY),
                    prov = input.getStringByField(DEC_PROVINCE),
                    rowKey = input.getStringByField(DEC_ROW_KEY),
                    basicInfo = input.getStringByField(DEC_BASIC_INFO),
                    userTxt = input.getStringByField(DEC_USER_TXT),
                    agentTxt = input.getStringByField(DEC_AGENT_TXT),
                    allTxt = input.getStringByField(DEC_ALL_TXT);
            String month = day.substring(0, 6);
            String realPath = this.oriPath.replace("{prov}", prov).replace("{month}", month);
            String content = rowKey + DELIMITER_BLOCK + basicInfo + DELIMITER_BLOCK + allTxt + "\r\n";
            try {
                TopologyHelper.appendHDFS(realPath, content, this.hadoopConf);
                collector.emit(TOPOLOGY_STREAM_STORE_ORI_ID, new Values(
                   rowKey, prov, day, basicInfo, userTxt, agentTxt, allTxt
                ));
            }catch (IOException e) {
                logger.error("store origin to hdfs has error", e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                TOPOLOGY_STREAM_STORE_ORI_ID,
                new Fields(DEC_ROW_KEY, DEC_PROVINCE, DEC_DAY,
                        DEC_BASIC_INFO, DEC_USER_TXT, DEC_AGENT_TXT, DEC_ALL_TXT)
        );
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
