package com.alleyz.tsp.topo;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.alleyz.tsp.config.ConfigUtil;
import com.alleyz.tsp.topo.constant.TopoConstant;
import com.alleyz.tsp.topo.logocal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.alleyz.tsp.topo.constant.TopoConstant.*;
import static com.alleyz.tsp.topo.logocal.TxtSpout.NAME;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class TxtTopology {
    public final static String TOPOLOGY_NAME = "record-topology";
    private static Logger logger = LoggerFactory.getLogger(TxtTopology.class);

    private static void drawTopology(TopologyBuilder topology, Map<String, Object> conf) {

        SpoutDeclarer txtSpout = topology.setSpout(NAME, new TxtSpout(),
                ConfigUtil.getIntVal(conf, TOPOLOGY_PARALLEL_SPOUT, 1));

        BoltDeclarer newBolt = topology.setBolt(NewWordBolt.NAME, new NewWordBolt(),
                ConfigUtil.getIntVal(conf, TOPOLOGY_PARALLEL_BOLT_NW, 1));

        BoltDeclarer segBolt = topology.setBolt(SegmentBolt.NAME, new SegmentBolt(),
                ConfigUtil.getIntVal(conf, TOPOLOGY_PARALLEL_BOLT_SEG, 1));

        BoltDeclarer idxBolt = topology.setBolt(IndexBolt.NAME, new IndexBolt(),
                ConfigUtil.getIntVal(conf, TOPOLOGY_PARALLEL_BOLT_INDEX, 1));

        BoltDeclarer hdfsBolt = topology.setBolt(StoredHDFSBolt.NAME, new StoredHDFSBolt(),
                ConfigUtil.getIntVal(conf, TOPOLOGY_PARALLEL_BOLT_HDFS, 1));

//        BoltDeclarer qcBolt = topology.setBolt(QualityBolt.NAME, new QualityBolt(), 1);
//        BoltDeclarer oraBolt = topology.setBolt(StoreOracleBolt.NAME, new StoreOracleBolt(), 1);

        newBolt.allGrouping(TxtSpout.NAME, TopoConstant.TOPOLOGY_STREAM_TXT_ID);
        segBolt.allGrouping(TxtSpout.NAME, TopoConstant.TOPOLOGY_STREAM_TXT_ID);
        idxBolt.allGrouping(TxtSpout.NAME, TopoConstant.TOPOLOGY_STREAM_TXT_ID);

        hdfsBolt.allGrouping(TxtSpout.NAME, TopoConstant.TOPOLOGY_STREAM_TXT_ID)
                .allGrouping(NewWordBolt.NAME, TopoConstant.TOPOLOGY_STREAM_NEW_WORD_ID)
                .allGrouping(SegmentBolt.NAME, TopoConstant.TOPOLOGY_STREAM_SEG_WORD_ID);
//        qcBolt.allGrouping(TxtSpout.NAME, TopoConstant.TOPOLOGY_STREAM_TXT_ID);
//        oraBolt.allGrouping(StoreOracleBolt.NAME, TopoConstant.TOPOLOGY_STREAM_QC_ID);

    }
    private static void runLocalMode(Map<String, Object> conf) throws InterruptedException{
        try {
            TopologyBuilder builder = new TopologyBuilder();
            LocalCluster cluster = new LocalCluster();

            drawTopology(builder, conf);

            cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());

            Thread.sleep(600000);
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runRemoteMode(Map<String, Object> conf) throws AlreadyAliveException, InvalidTopologyException{
        TopologyBuilder builder = new TopologyBuilder();

        drawTopology(builder, conf);

        StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
    }

    public static void main(String[] args) {
        try {
            if ("local".equals(ConfigUtil.getStrVal(TopoConstant.TOPOLOGY_MODE))) {
                runLocalMode(ConfigUtil.prop2Map("/topology-local.properties"));
            } else {
                runRemoteMode(ConfigUtil.prop2Map("/topology-remote.properties"));
            }
        }catch (InterruptedException e) {
            e.printStackTrace();
        }catch (AlreadyAliveException e) {
            System.out.println("已存在同名拓扑");
            e.printStackTrace();
        }catch (InvalidTopologyException e) {
            System.out.println("错误拓扑");
            e.printStackTrace();
        }
    }
}

