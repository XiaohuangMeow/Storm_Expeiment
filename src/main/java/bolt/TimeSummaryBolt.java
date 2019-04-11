package bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TimeSummaryBolt extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	private Map<Integer, Double> map=new HashMap<Integer, Double>();
	private OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		this.map=new HashMap<Integer, Double>();
	}

	public void execute(Tuple input) {
		int seq=input.getInteger(0);
		double t=input.getDouble(1);
		if (map.containsKey(seq)) {
			map.put(seq, t+map.get(seq));
		}
		else {
			map.put(seq, t);
		}
		collector.emit(new Values("path"+seq,map.get(seq)));
//		System.out.println(seq+" "+t+" "+map.get(seq));
	}
	

	@Override
	public void cleanup(){
		for (Map.Entry<Integer, Double> entry:map.entrySet()) {
			System.out.println("path"+entry.getKey()+" totalTime="+entry.getValue());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
		declarer.declare(new Fields("seq","time"));
	}


}
