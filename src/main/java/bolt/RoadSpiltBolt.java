package bolt;


import java.util.List;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import cern.colt.matrix.DoubleMatrix3D;
import cern.colt.matrix.impl.SparseDoubleMatrix3D;
import data.PlaceInfomation;

public class RoadSpiltBolt extends BaseBasicBolt{
	
	@SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
//		collector.emit(new Values(seq,q,A,queryslot,places));
		int seq=input.getInteger(0);
		List<Integer> q=(List<Integer>) input.getValue(1);
		DoubleMatrix3D A=(DoubleMatrix3D)input.getValue(2);
		int queryslot=input.getInteger(3);
		List<PlaceInfomation> places=(List<PlaceInfomation>)input.getValue(4);
		DoubleMatrix3D a=new SparseDoubleMatrix3D(3, A.rows(), A.columns());
		for (int i=0;i<A.rows();i++) {
			for (int j=0;j<A.columns();j++) {
				a.setQuick(0, i, j, A.getQuick((queryslot+47)%48, i, j));
				a.setQuick(1, i, j, A.getQuick(queryslot, i, j));
				a.setQuick(2, i, j, A.getQuick((queryslot+1)%48, i, j));
			}
		}
		for (int i=0;i<q.size()-1;i++) {
			int x=q.get(i);
			int y=q.get(i+1);
			collector.emit(new Values(seq,x,y,a,places));
//			System.out.println(seq+" "+x+" "+y);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("seq","x","y","a","places"));
	}


}
