package com.alleyz.tsp.topo.batch;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.ICommitter;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by alleyz on 2017/6/1.
 */
public class StoreHBaseBatchBolt implements IBasicBolt, ICommitter {

    private final ConcurrentMap<Long, Queue<Long>> dataMaps = new ConcurrentHashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
//        if("streamId".equals(input.getSourceStreamId())) {
            BatchId batchId = (BatchId) input.getValue(0);
            Long val = (Long) input.getValue(1);
            Queue<Long> queue = dataMaps.get(batchId.getId());
            if (queue == null) {
                synchronized (dataMaps) {
                    queue = dataMaps.get(batchId.getId());
                    if (queue == null) {
                        queue = new ConcurrentLinkedQueue<>();
                        dataMaps.put(batchId.getId(), queue);
                    }
                }
            }
            queue.add(val);
            dataMaps.put(batchId.getId(), queue);
            collector.emit(new Values(batchId, val));
            System.out.println(batchId + "+++++++++ emit " + Thread.currentThread().getName());
//        }
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
        System.out.println(id + "=============== maps size = " + dataMaps.size());
        dataMaps.get(id.getId()).forEach(System.out::println);
        try { Thread.sleep(3000);}catch (Exception e) {e.printStackTrace();}
        dataMaps.remove(id.getId());
        System.out.println(id + "--------------- maps size = " + dataMaps.size());
        String success = "success";
        return success.getBytes();
    }

    @Override
    public void revert(BatchId id, byte[] commitResult) {
        System.out.println(id + "  revert =================== " + new String(commitResult));
    }
}
