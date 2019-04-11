package org.apache.storm.Storm_Experiment;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;


import bolt.ExtimateRoad;
import bolt.RoadSpiltBolt;
import bolt.gatherBolt;
import spout.dataSpout;

public class App {
	
	private static final String dataSpoutID = "data_spout";
    private static final String RoadSpiltBoltID = "road_spilt";
    private static final String ExtimateRoadID = "time_extimate";
    private static final String gatherBoltID = "gather_time";
    private static final String TopologyBuilderID = "topology";


	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		dataSpout dataspout=new dataSpout();
		RoadSpiltBolt roadspiltbolt=new RoadSpiltBolt();
		ExtimateRoad extimateroad=new ExtimateRoad();
		gatherBolt gatherbolt=new gatherBolt();
		TopologyBuilder builder=new TopologyBuilder();
		builder.setSpout(dataSpoutID, dataspout);
		builder.setBolt(RoadSpiltBoltID, roadspiltbolt,2).shuffleGrouping(dataSpoutID);
		builder.setBolt(ExtimateRoadID, extimateroad,4).shuffleGrouping(RoadSpiltBoltID);
		builder.setBolt(gatherBoltID, gatherbolt).fieldsGrouping(ExtimateRoadID, new Fields("seq"));
		
		
		Config config=new Config();
		config.setDebug(false);
		StormSubmitter.submitTopology(TopologyBuilderID, config, builder.createTopology());
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TopologyBuilderID, config, builder.createTopology());
        Utils.sleep(30000);
        cluster.killTopology(TopologyBuilderID);        
        cluster.shutdown();
		
	}
}
