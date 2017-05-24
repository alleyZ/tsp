package com.alleyz.tsp;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.alleyz.tsp.config.ConfigUtil;
import com.alleyz.tsp.topo.TxtTopology;
import com.alleyz.tsp.topo.constant.TopoConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by alleyz on 2017/5/24.
 *
 */
public class Application {
    private static Logger logger = LoggerFactory.getLogger(Application.class);

    /**
     * 本地运行模式
     * @param conf 配置
     * @throws InterruptedException 异常
     */
    private static void runLocalMode(Map<String, Object> conf) throws InterruptedException{
        try {
            TopologyBuilder builder = new TopologyBuilder();
            LocalCluster cluster = new LocalCluster();

            TxtTopology.drawTopology(builder, conf);
            logger.info("commit Topology ...");
            cluster.submitTopology(TxtTopology.TOPOLOGY_NAME, conf, builder.createTopology());

            logger.info("commit Topology finish, sleep 600000");
            Thread.sleep(600000);
            cluster.killTopology(TxtTopology.TOPOLOGY_NAME);
            cluster.shutdown();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 远程运行模式
     * @param conf 配置
     * @throws AlreadyAliveException
     * @throws InvalidTopologyException
     */
    private static void runRemoteMode(Map<String, Object> conf) throws AlreadyAliveException, InvalidTopologyException{
        TopologyBuilder builder = new TopologyBuilder();

        TxtTopology.drawTopology(builder, conf);

        StormSubmitter.submitTopology(TxtTopology.TOPOLOGY_NAME, conf, builder.createTopology());
    }

    public static void main(String[] args) {
        try {
            if ("local".equals(ConfigUtil.getStrVal(TopoConstant.TOPOLOGY_MODE))) {
                runLocalMode(ConfigUtil.prop2Map("/topology-local.properties"));
            } else {
                runRemoteMode(ConfigUtil.prop2Map("/topology-remote.properties"));
            }
        }catch (InterruptedException e) {
            logger.error("InterruptedException ", e);
        }catch (AlreadyAliveException e) {
            logger.error("AlreadyAliveException ", e);
            e.printStackTrace();
        }catch (InvalidTopologyException e) {
            logger.error("InvalidTopologyException ", e);
            e.printStackTrace();
        }
    }
}
