package com.alleyz.tsp.topo.batch;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleHelpers;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.IBatchSpout;

import java.util.Map;

/**
 * Created by alleyz on 2017/6/1.
 */
public class TxtBatchSpout implements IBatchSpout {
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TupleHelpers.isTickTuple(input)) return;

        System.out.println(input.getSourceStreamId());
//        if()
        BatchId batchId = (BatchId) input.getValue(0);
        System.out.println();
        System.out.println("-----sssss-------~ execute ~~" + batchId);
        System.out.println();
        if(batchId.getId() < 4)
        for (int i = 0; i < 5; i++) {
//            long value = rand.nextInt(10);
            collector.emit(new Values(batchId, Long.parseLong(i + "")));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("BatchId", "value"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public byte[] commit(BatchId id) throws FailedException {
        return null;
    }

    @Override
    public void revert(BatchId id, byte[] commitResult) {

    }
}
