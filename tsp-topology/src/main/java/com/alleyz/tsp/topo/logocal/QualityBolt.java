package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alleyz.tsp.config.ConfigUtil;
import com.alleyz.tsp.constant.Constant;
import com.alleyz.tsp.topo.constant.TopoConstant;
import com.ql.util.express.DefaultContext;
import com.ql.util.express.ExpressRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alleyz.tsp.topo.constant.TopoConstant.DEC_QC_ITEM;
import static com.alleyz.tsp.topo.constant.TopoConstant.DEC_ROW_KEY;
import static com.alleyz.tsp.topo.constant.TopoConstant.TOPOLOGY_STREAM_QC_ID;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class QualityBolt implements IBasicBolt{
    public static final String NAME = "quaBolt";
    private String qlExpress;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try{
            qlExpress = ConfigUtil.getExpress("/qc-level.ql");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            if (TopoConstant.TOPOLOGY_STREAM_TXT_ID.equals(input.getSourceStreamId())) {
                ExpressRunner runner = new ExpressRunner();
                String basicInfo = input.getStringByField(TopoConstant.DEC_BASIC_INFO);
                String[] info = basicInfo.split(Constant.DELIMITER_FIELDS);
                Map<String, Integer> maps = Constant.getHBaseMapping();
                String level = info[maps.get(Constant.custLevel)];
                String allTxt = info[maps.get(Constant.allContent)];
                DefaultContext<String, Object> ctx = new DefaultContext<>();
                ctx.put("level", level);
                ctx.put("allTxt", allTxt);
                List<String> errList = new ArrayList<>();
                Boolean isQc = (Boolean) runner.execute(qlExpress, ctx, errList, true, false);
                if (isQc) {
                    String rowKey = input.getStringByField(DEC_ROW_KEY);
                    collector.emit(TOPOLOGY_STREAM_QC_ID, new Values(rowKey, "level-qc"));
                }
            }

        }catch (Exception e) {
            collector.reportError(e);
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TOPOLOGY_STREAM_QC_ID, new Fields(DEC_ROW_KEY, DEC_QC_ITEM));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
