package com.alleyz.tsp.nw;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.nw.topo.NWTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by alleyz on 2017/5/31.
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

            NWTopology.drawTopology(builder, conf);
            logger.info("commit Topology ...");
            cluster.submitTopology(NWTopology.NAME, conf, builder.createTopology());

            logger.info("commit Topology finish, sleep 600000");
            Thread.sleep(600000);
            cluster.killTopology(NWTopology.NAME);
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

        NWTopology.drawTopology(builder, conf);

        StormSubmitter.submitTopology(NWTopology.NAME, conf, builder.createTopology());
    }

    public static void main(String[] args) {
        try {
            if (args !=null && args.length >0 &&"local".equals(args[0])) {
                runLocalMode(ConfigUtils.prop2Map("/topology-local.properties"));
            } else {
                runRemoteMode(ConfigUtils.prop2Map("/topology-remote.properties"));
            }
        }catch (InterruptedException | AlreadyAliveException | InvalidTopologyException e) {
            logger.error("InterruptedException ", e);
        }
    }
}
