package spout;

import data.PlaceInfomation;
import data.Trajectory;
import data.qustionList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.csvreader.CsvReader;

import cern.colt.matrix.DoubleMatrix3D;
import cern.colt.matrix.impl.SparseDoubleMatrix3D;

public class dataSpout extends BaseRichSpout{
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private boolean completed = false;
	
	private static Map<String,Integer> stationNameId=new HashMap<String, Integer>(); 
	private static Map<Integer, Integer> IdOrder=new HashMap<Integer, Integer>();
	private static List<Trajectory> trajectories=new ArrayList<Trajectory>();
	

	private static int getslot(String time) {
		String temp=time.split(" ")[1];
		String[] HMS=temp.split(":");
		int hour=Integer.valueOf(HMS[0]);
		int minute=Integer.valueOf(HMS[1]);
		return hour*2+(minute<30?0:1);
	}
	
	private static int getslot2(String time) {
		String[] HMS=time.split(":");
		int hour=Integer.valueOf(HMS[0]);
		int minute=Integer.valueOf(HMS[1]);
		return hour*2+(minute<30?0:1);
	}
	
	
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
	}

	public static DoubleMatrix3D constructTensor(int num,List<Trajectory> trajectories, Map<Integer, Integer> IdOrder) {
		DoubleMatrix3D A=new SparseDoubleMatrix3D(48,num,num);
		int[][][] count = new int[48][330][330];
		for (int i = 0; i < trajectories.size(); i++) {
			int from = trajectories.get(i).from;
			int to = trajectories.get(i).to;
			int slot = trajectories.get(i).slot;
			double cost = trajectories.get(i).cost;
			int fromseq = IdOrder.get(from);
			int toseq = IdOrder.get(to);

			if (A.getQuick(slot, fromseq, toseq) == 0) {
				A.set(slot, fromseq, toseq, cost);
				count[slot][fromseq][toseq]++;
			} else {
				int c = count[slot][fromseq][toseq];
				A.set(slot, fromseq, toseq, (cost + A.get(slot, fromseq, toseq) * c) / (c + 1));
			}
		}
		return A;
	}
	
	public void nextTuple() {
		if(completed){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			
			return;
		}		
		List<PlaceInfomation> places=new ArrayList<PlaceInfomation>();
		try {
			String path = "src/main/resources/2013-10 - Citi Bike trip data.csv";
			CsvReader csvReader = new CsvReader(path);
			csvReader.readHeaders();
			int cnt=0;
			while (csvReader.readRecord()) {
				int cost=Integer.valueOf(csvReader.get(0));
				int from=Integer.valueOf(csvReader.get(3));
				String FromName=csvReader.get(4);
				int to=Integer.valueOf(csvReader.get(7));
				String ToName=csvReader.get(8);
				stationNameId.put(FromName,from);
				stationNameId.put(ToName,to);
				double FromLatitude=Double.valueOf(csvReader.get(5));
				double ToLatitude=Double.valueOf(csvReader.get(9));
				double FromLongtitude=Double.valueOf(csvReader.get(6));
				double ToLongtitude=Double.valueOf(csvReader.get(10));
				String time=csvReader.get(1);
				if (!IdOrder.containsKey(from)) {
					IdOrder.put(from, cnt);
					places.add(new PlaceInfomation(FromName, FromLatitude, FromLongtitude, from));
					cnt++;
				}
				if (!IdOrder.containsKey(to)) {
					IdOrder.put(to, cnt);
					places.add(new PlaceInfomation(ToName, ToLatitude, ToLongtitude, to));
					cnt++;
				}
				int slot=getslot(time);
				trajectories.add(new Trajectory(cost, from, to, slot));
			}
//			System.out.println(cnt);
			DoubleMatrix3D A=constructTensor(cnt,trajectories, IdOrder);
			String queryPath="src/main/resources/query.csv";
			csvReader = new CsvReader(queryPath);
			csvReader.readHeaders();
			int seq=0;
			List<qustionList> QL=new ArrayList<qustionList>();
			while (csvReader.readRecord()) {
				int queryslot=getslot2(csvReader.get(1));
				int road=Integer.valueOf(csvReader.get(2));
				List<Integer> q=new ArrayList<Integer>();
				for (int i=0;i<road;i++) {
					q.add(IdOrder.get(stationNameId.get(csvReader.get(i+3))));
				}
				qustionList ql=new qustionList(seq, q, queryslot);
//				collector.emit(new Values(seq,q,A,queryslot,places));
//				System.out.println(seq+" "+q+" "+queryslot);
				QL.add(ql);
				seq++;
				if (seq%10000==0) {
					collector.emit(new Values(QL,A,places));
					QL=new ArrayList<qustionList>();
				}
			}
			if (QL.size()!=0) {
//				for (int i=0;i<QL.size();i++) {
//					System.out.println("QLseq="+QL.get(i).seq);
//				}
				collector.emit(new Values(QL,A,places));
			}
			csvReader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			completed=true;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("QL","tensor","places"));
//		declarer.declare(new Fields("seq","query","tensor","queryslot","places"));
	}

}
