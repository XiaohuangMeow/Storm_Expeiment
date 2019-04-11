package bolt;

import java.util.List;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import cern.colt.matrix.impl.DenseDoubleMatrix1D;
import cern.colt.matrix.linalg.Algebra;
import data.DistanceTime;
import data.KdTree;
import data.PlaceInfomation;
import data.Point;
import data.qustionList;

public class PathSplitBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
//		declarer.declare(new Fields("qlst","points","dts"));
		List<qustionList> qlst=(List<qustionList>) input.getValue(0); 
		List<Point> points=(List<Point>) input.getValue(1);
		List<DistanceTime> dts=(List<DistanceTime>) input.getValue(2);
		List<PlaceInfomation> places=(List<PlaceInfomation>) input.getValue(3);
		KdTree kdtree=new KdTree(points);
		int n=dts.size();
		double[] x=new double[n];
		double[] y=new double[n];
		for (int i=0;i<n;i++) {
			x[i]=dts.get(i).distance;
			y[i]=dts.get(i).time;
		}
		DenseDoubleMatrix1D X=new DenseDoubleMatrix1D(x);
		DenseDoubleMatrix1D Y=new DenseDoubleMatrix1D(y);
		double theta = (1 / Algebra.DEFAULT.mult(X, X)) * Algebra.DEFAULT.mult(X, Y);
		for (int i=0;i<qlst.size();i++) {
			int seq=qlst.get(i).seq;
			List<Integer> q=qlst.get(i).q;
			int queryslot=qlst.get(i).queryslot;
			collector.emit(new Values(seq,q,queryslot,theta,kdtree,places));
//			System.out.println("seq="+seq);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("seq","q","queryslot","theta","kdtree","places"));
	}
	

}
