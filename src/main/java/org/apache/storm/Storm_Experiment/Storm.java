package org.apache.storm.Storm_Experiment;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.utils.Utils;

import bolt.ConstructNearTimeBolt;
import bolt.PathSplitBolt;
import bolt.SpiltIntoOneRoadBolt;
import bolt.TimeExtimateBolt;
import bolt.TimeSummaryBolt;
import redis.clients.jedis.Jedis;
import spout.dataSpout;

public class Storm {
	
	public static class pathStoreMapper implements RedisStoreMapper{

		private static final long serialVersionUID = 1L;
		private RedisDataTypeDescription description;
		private final String hashKey="pathTime";
		
		public pathStoreMapper() {
			description=new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
		}
		
		public String getKeyFromTuple(ITuple tuple) {
			return String.valueOf(tuple.getStringByField("seq"));
		}

		public String getValueFromTuple(ITuple tuple) {
			return String.valueOf(tuple.getDoubleByField("time"));
		}

		public RedisDataTypeDescription getDataTypeDescription() {
			return description;
		}
		
	}
	
	private static final String dataSpoutID = "data_spout";
	private static final String ConstructNearTimeBoltID="construct_near_time";
    private static final String PathSpiltID = "path_spilt";
    private static final String SpiltIntoOneRoadID = "spilt_into_one_road";
    private static final String TimeExtimateID = "time_extimate";
    private static final String TimeSummaryID = "time_summary";
    private static final String TopologyBuilderID = "topology";


	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		
        @SuppressWarnings("resource")
		Jedis jedis = new Jedis("localhost",6379,100000);
        System.out.println("服务正在运行: "+jedis.ping());
				
		dataSpout dataspout=new dataSpout();
		ConstructNearTimeBolt constructNearTimeBolt=new ConstructNearTimeBolt();
		PathSplitBolt pathSplitBolt=new PathSplitBolt();
		SpiltIntoOneRoadBolt spiltIntoOneRoadBolt=new SpiltIntoOneRoadBolt();
		TimeExtimateBolt timeExtimateBolt=new TimeExtimateBolt();
		TimeSummaryBolt timeSummaryBolt=new TimeSummaryBolt();
		TopologyBuilder builder=new TopologyBuilder();
		builder.setSpout(dataSpoutID, dataspout);
		builder.setBolt(ConstructNearTimeBoltID, constructNearTimeBolt,2).shuffleGrouping(dataSpoutID);
		builder.setBolt(PathSpiltID, pathSplitBolt,2).shuffleGrouping(ConstructNearTimeBoltID);
		builder.setBolt(SpiltIntoOneRoadID, spiltIntoOneRoadBolt,4).shuffleGrouping(PathSpiltID);
		builder.setBolt(TimeExtimateID, timeExtimateBolt,4).shuffleGrouping(SpiltIntoOneRoadID);
		builder.setBolt(TimeSummaryID, timeSummaryBolt,4).fieldsGrouping(TimeExtimateID,new Fields("seq"));		

		JedisPoolConfig poolConfig=new JedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();
		RedisStoreMapper storeMapper=new pathStoreMapper();
		RedisStoreBolt storeBolt=new RedisStoreBolt(poolConfig, storeMapper);
		builder.setBolt("RedisStoreBolt", storeBolt).globalGrouping(TimeSummaryID);
		
		Config config=new Config();		
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TopologyBuilderID, config, builder.createTopology());
            Utils.sleep(12000);
            cluster.killTopology(TopologyBuilderID);
            cluster.shutdown();
        } else {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
		
	}
}
