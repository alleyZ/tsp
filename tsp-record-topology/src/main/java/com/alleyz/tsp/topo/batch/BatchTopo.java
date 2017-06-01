package com.alleyz.tsp.topo.batch;

import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.jstorm.batch.BatchTopologyBuilder;
import com.alleyz.tsp.config.ConfigUtils;

/**
 * Created by alleyz on 2017/6/1.
 */
public class BatchTopo {


    public static TopologyBuilder builder() {
        BatchTopologyBuilder builder = new BatchTopologyBuilder("abjs");

//        BoltDeclarer boltDeclarer = builder.setSpout("Spout", new TxtBatchSpout(), 1);
//        builder.setBolt("Bolt", new StoreHBaseBatchBolt(), 1).shuffleGrouping("Spout", "streamId");
        BoltDeclarer boltDeclarer = builder.setSpout("Spout", new TxtBatchSpout(), 1);
        builder.setBolt("Bolt", new StoreHBaseBatchBolt(), 1).shuffleGrouping("Spout");
        return builder.getTopologyBuilder();
    }

    public static void main(String[] args) {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("abjs", ConfigUtils.prop2Map("/topology-local.properties"), builder().createTopology());
    }
}
