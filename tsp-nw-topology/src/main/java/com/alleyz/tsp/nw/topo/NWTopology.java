package com.alleyz.tsp.nw.topo;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.alleyz.tsp.nw.constant.Constant;
import com.alleyz.tsp.nw.logic.NewWordBolt;
import com.alleyz.tsp.nw.logic.Store2Redis;
import com.alleyz.tsp.nw.logic.TxtSpout;

import java.util.Map;

/**
 * Created by alleyz on 2017/5/31.
 *
 */
public class NWTopology {
    public final static String NAME = "new-word-topology";
    public static void drawTopology(TopologyBuilder topology, Map<String, Object> conf){
        SpoutDeclarer spout = topology.setSpout(TxtSpout.NAME, new TxtSpout(), 1);
        BoltDeclarer nwBolt = topology.setBolt(NewWordBolt.NAME, new NewWordBolt(), 1);
        BoltDeclarer redisBolt = topology.setBolt(Store2Redis.NAME, new Store2Redis(), 1);


        nwBolt.allGrouping(TxtSpout.NAME, Constant.TOPOLOGY_STREAM_NW_SPOUT_ID);
        redisBolt.allGrouping(NewWordBolt.NAME, Constant.TOPOLOGY_STREAM_NW_ID);
    }
}
