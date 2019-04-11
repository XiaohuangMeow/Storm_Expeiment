package bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class gatherBolt extends BaseRichBolt {
	
	private Map<Integer, Double> map=new HashMap<Integer, Double>();

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.map=new HashMap<Integer, Double>();
	}

	public void execute(Tuple input) {
		int q=input.getInteger(0);
		double t=input.getDouble(1);
		if (map.containsKey(q)) {
			map.put(q, t+map.get(q));
		}
		else {
			map.put(q, t);
		}
	}
	

	@Override
	public void cleanup(){
		for (Map.Entry<Integer, Double> entry:map.entrySet()) {
			System.out.println("path"+entry.getKey()+" totalTime="+entry.getValue());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
	}


}
