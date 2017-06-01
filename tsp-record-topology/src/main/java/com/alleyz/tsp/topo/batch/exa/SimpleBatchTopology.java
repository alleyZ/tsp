/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alleyz.tsp.topo.batch.exa;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.jstorm.batch.BatchTopologyBuilder;
import com.alleyz.tsp.topo.batch.StoreHBaseBatchBolt;
import com.alleyz.tsp.topo.batch.TxtBatchSpout;

public class SimpleBatchTopology {
    
    static boolean isLocal = true;
    static String  topologyName;
    


    public static TopologyBuilder builder() {
        BatchTopologyBuilder builder = new BatchTopologyBuilder("abjs");

//        BoltDeclarer boltDeclarer = builder.setSpout("Spout", new TxtBatchBolt(), 1);
//        builder.setBolt("Bolt", new StoreHBaseBatchBolt(), 1).shuffleGrouping("Spout", "streamId");
        BoltDeclarer boltDeclarer = builder.setSpout("Spout", new TxtBatchSpout(), 1);
        builder.setBolt("Bolt", new StoreHBaseBatchBolt(), 1).shuffleGrouping("Spout");
        return builder.getTopologyBuilder();
    }

    public static void main(String[] args) {
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        cluster.submitTopology("abjs", conf, builder().createTopology());
    }
}
