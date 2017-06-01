package com.alleyz.tsp.topo.logic;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alleyz.tsp.helper.JdbcHelper;
import com.alleyz.tsp.topo.constant.TopoConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class StoreOracleBolt implements IBasicBolt {
    private static Logger logger = LoggerFactory.getLogger(StoreOracleBolt.class);
    public static final String NAME = "oraBolt";
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (TopoConstant.TOPOLOGY_STREAM_QC_ID.equals(input.getSourceStreamId())){
            String rowKey = input.getStringByField(TopoConstant.DEC_ROW_KEY);
            String qcItem = input.getStringByField(TopoConstant.DEC_QC_ITEM);
            String prov = input.getStringByField(TopoConstant.DEC_PROVINCE);
            try {
                JdbcHelper.getInstance().insert("insert into tbl_voc_qc_row(row_id, row_key, prov, item)" +
                        "values(seq_qc_row.nextval, ?, ?, ?)", rowKey, prov, qcItem);
            }catch (Exception e){
                logger.error("ora", e);
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
