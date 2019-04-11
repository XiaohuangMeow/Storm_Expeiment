package bolt;

import java.util.List;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import data.KdTree;
import data.PlaceInfomation;

public class SpiltIntoOneRoadBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
//		collector.emit(new Values(seq,q,queryslot,theta,kdtree));
		int seq=input.getInteger(0);
		List<Integer> q=(List<Integer>) input.getValue(1);
//		int queryslot=input.getInteger(2);
		double theta=input.getDouble(3);
		KdTree kdtree=(KdTree) input.getValue(4);
		List<PlaceInfomation> places=(List<PlaceInfomation>) input.getValue(5);
		for (int i=0;i<q.size()-1;i++) {
			int x=q.get(i);
			int y=q.get(i+1);
			collector.emit(new Values(seq,x,y,theta,kdtree,places));
//			System.out.println(seq+" "+x+" "+y+" ");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("seq","x","y","theta","kdtree","places"));
	}

}
