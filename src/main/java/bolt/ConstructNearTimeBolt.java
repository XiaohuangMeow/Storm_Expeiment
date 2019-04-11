package bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import cern.colt.matrix.DoubleMatrix3D;
import data.Point;
import data.DistanceTime;
import data.PlaceInfomation;
import data.qustionList;

public class ConstructNearTimeBolt  extends BaseBasicBolt{
	
	private static final long serialVersionUID = 1L;

	public static double computeDistance(double fromLatitude, double fromLongtitude, double toLatitude,
			double toLongtitude) {

		final int R = 6371; // Radius of the earth

		double latDistance = Math.toRadians(toLatitude - fromLatitude);
		double lonDistance = Math.toRadians(toLongtitude - fromLongtitude);
		double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(fromLatitude))
				* Math.cos(Math.toRadians(toLatitude)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double distance = R * c * 1000; // convert to meters
		distance = Math.pow(distance, 2);
		return Math.sqrt(distance);
	}
	
	@SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
//		collector.emit(new Values(QL,A,places));
//		double[] a=new double[10];
		List<qustionList> QL=(List<qustionList>) input.getValue(0);
		Collections.sort(QL, new Comparator<qustionList>() {
			public int compare(qustionList o1, qustionList o2) {
				return o1.queryslot-o2.queryslot;
			}
		});
		DoubleMatrix3D A=(DoubleMatrix3D) input.getValue(1);
		List<PlaceInfomation> places=(List<PlaceInfomation>) input.getValue(2);
		List<List<Point>> lst=new ArrayList<List<Point>>();
		List<List<DistanceTime>> distanceTime=new ArrayList<List<DistanceTime>>();
		for (int k=0;k<48;k++) {
			List<Point> l=new ArrayList<Point>();
			List<DistanceTime> dt=new ArrayList<DistanceTime>();
			for (int i=0;i<A.rows();i++) {
				for (int j=0;j<A.columns();j++) {
					if (A.getQuick(k, i, j)!=0) {
						PlaceInfomation p1=places.get(i);
						PlaceInfomation p2=places.get(j);
						Point o=new Point(A.getQuick(k, i, j),p1, p2);
						l.add(o);
						dt.add(new DistanceTime(computeDistance(p1.latitude, p1.longitude, p2.latitude, p2.longitude), A.get(k, i, j)));
					}
				}
			}
			lst.add(l);
			distanceTime.add(dt);
		}
		
		List<qustionList> qlst=new ArrayList<qustionList>();
		int cnt=QL.get(0).queryslot;
		for (int i=0;i<QL.size();i++) {
			if (QL.get(i).queryslot==cnt) {
				qlst.add(QL.get(i));
			}
			else {
				int k1=(cnt+47)%48;
				int k2=(cnt+1)%48;
				List<Point> points=new ArrayList<Point>();
				points.addAll(lst.get(k1));
				points.addAll(lst.get(cnt));
				points.addAll(lst.get(k2));
				List<DistanceTime> dts=new ArrayList<DistanceTime>();
				dts.addAll(distanceTime.get(k1));
				dts.addAll(distanceTime.get(cnt));
				dts.addAll(distanceTime.get(k2));
				collector.emit(new Values(qlst,points,dts,places));
				cnt=QL.get(i).queryslot;
				qlst=new ArrayList<qustionList>();
				qlst.add(QL.get(i));
			}
			if (i==QL.size()-1) {
				int k1=(cnt+47)%48;
				int k2=(cnt+1)%48;
				List<Point> points=new ArrayList<Point>();
				points.addAll(lst.get(k1));
				points.addAll(lst.get(cnt));
				points.addAll(lst.get(k2));
				List<DistanceTime> dts=new ArrayList<DistanceTime>();
				dts.addAll(distanceTime.get(k1));
				dts.addAll(distanceTime.get(cnt));
				dts.addAll(distanceTime.get(k2));
				collector.emit(new Values(qlst,points,dts,places));
			}
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("qlst","points","dts","places"));
	}

}
