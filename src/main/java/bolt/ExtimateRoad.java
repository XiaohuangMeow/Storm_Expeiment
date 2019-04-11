package bolt;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import cern.colt.matrix.DoubleMatrix3D;
import data.PlaceInfomation;

public class ExtimateRoad extends BaseBasicBolt{

	private static int knn_k=5;
	private static int par1=33;
	private static int par2=17;
	private static int par3=19;
	private static int par4=33;
	
	private double computeDistance(double fromLatitude, double fromLongtitude, double toLatitude,
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

	private double kc(double distancecha,double Lacha1,double Lacha2,double longcha1,double longcha2) {
		return Math.abs(distancecha)/1000+Math.abs(Lacha1)*par1+Math.abs(longcha1)*par2+Math.abs(Lacha2)*par3+Math.abs(longcha2)*par4;
	}
	
	@SuppressWarnings("unchecked" )
	public void execute(Tuple input, BasicOutputCollector collector) {
		int seq=input.getInteger(0);
		int x=input.getInteger(1);
		int y=input.getInteger(2);
		DoubleMatrix3D a=(DoubleMatrix3D)input.getValue(3);
		List<PlaceInfomation> places=(List<PlaceInfomation>)input.getValue(4);
		double distance=computeDistance(places.get(x).latitude, places.get(x).longitude, places.get(y).latitude, places.get(y).longitude);
				
		Map<Double, Double> map=new TreeMap<Double, Double>();
		for (int i=0;i<a.rows();i++) {
			for (int j=0;j<a.columns();j++) {
				if (a.get(0, i, j)!=0) {
					double distancetemp=computeDistance(places.get(i).latitude, places.get(i).longitude, places.get(j).latitude, places.get(j).longitude);
					double knnDistance=kc(distance-distancetemp,
							places.get(x).latitude-places.get(i).latitude,
							places.get(y).latitude-places.get(j).latitude,
							places.get(x).longitude-places.get(i).longitude,
							places.get(y).longitude-places.get(j).longitude
							);
					map.put(knnDistance, a.get(0, i, j));
				}
				if (a.get(1, i, j)!=0) {
					double distancetemp=computeDistance(places.get(i).latitude, places.get(i).longitude, places.get(j).latitude, places.get(j).longitude);
					double knnDistance=kc(distance-distancetemp,
							places.get(x).latitude-places.get(i).latitude,
							places.get(y).latitude-places.get(j).latitude,
							places.get(x).longitude-places.get(i).longitude,
							places.get(y).longitude-places.get(j).longitude
							);
					map.put(knnDistance, a.get(0, i, j));
				}
				if (a.get(2, i, j)!=0) {
					double distancetemp=computeDistance(places.get(i).latitude, places.get(i).longitude, places.get(j).latitude, places.get(j).longitude);
					double knnDistance=kc(distance-distancetemp,
							places.get(x).latitude-places.get(i).latitude,
							places.get(y).latitude-places.get(j).latitude,
							places.get(x).longitude-places.get(i).longitude,
							places.get(y).longitude-places.get(j).longitude
							);
					map.put(knnDistance, a.get(0, i, j));
				}
			}
		}
		int cnt=0;
		double sum=0;
		for (double t:map.values()) {
			if (cnt>=knn_k) {
				break;
			}
			sum+=t;
			cnt++;
		}
		double result=sum/cnt;
		System.out.println(seq+" "+x+" "+y+" "+result);
		collector.emit(new Values(seq,result));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("seq","timeExtimate"));		
	}

}
