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
import data.Point;

public class TimeExtimateBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
//		collector.emit(new Values(seq,x,y,theta,kdtree,places));
		int seq=input.getInteger(0);
		int x=input.getInteger(1);
		int y=input.getInteger(2);
		double theta=input.getDouble(3);
		KdTree kdtree=(KdTree) input.getValue(4);
		List<PlaceInfomation> places=(List<PlaceInfomation>) input.getValue(5);
		Point point=new Point(0, places.get(x), places.get(y));
		List<Point> nearestpoints=kdtree.nearestNeighbourSearch(3, point);
		double distancesum=0;
		double timesum=0;
		for (int i=0;i<nearestpoints.size();i++) {
			distancesum+=nearestpoints.get(i).roadDistance();
			timesum+=nearestpoints.get(i).getx();
		}
		double theta2=timesum/distancesum;
		double extimate=theta*point.roadDistance()*0.7+theta2*point.roadDistance()*0.3;
		collector.emit(new Values(seq,extimate));
//		System.out.println("seq="+seq+" extimate="+extimate+" distance="+point.roadDistance());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("seq","extimate"));
	}

}
