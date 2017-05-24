package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alleyz.tsp.topo.constant.TopoConstant;
import com.alleyz.tsp.topo.utils.JdbcHelper;

import java.util.Map;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class StoreOracleBolt implements IBasicBolt {
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
                JdbcHelper.getInstance().insert("insert tbl_voc_qc_row(row_id, row_key, prov, item)" +
                        "values(seq_qc_row.nextval, ?, ?, ?)", rowKey, prov, qcItem);
            }catch (Exception e){
                e.printStackTrace();
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
