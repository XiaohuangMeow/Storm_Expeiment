package org.apache.storm.Storm_Experiment;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import data.Trajectory;

public class generateQueryData {
	
	private static Map<Integer,String> stationIdName=new HashMap<Integer, String>(); 
	private static List<Trajectory> trajectories=new ArrayList<Trajectory>();
	private static Map<Integer, Double> StationLatitude=new HashMap<Integer, Double>();
	private static Map<Integer, Double> StationLongtitude=new HashMap<Integer, Double>();
	private static Map<Integer, Integer> IdOrder=new HashMap<Integer, Integer>();
	private static Map<Integer, Integer> OrderId=new HashMap<Integer, Integer>();
	
	public static void readData() {
		try {
			String path = "2013-10 - Citi Bike trip data.csv";
			CsvReader csvReader = new CsvReader(path);
			csvReader.readHeaders();
			int cnt=0;
			while (csvReader.readRecord()) {
				int cost=Integer.valueOf(csvReader.get(0));
				int from=Integer.valueOf(csvReader.get(3));
				String FromName=csvReader.get(4);
				int to=Integer.valueOf(csvReader.get(7));
				String ToName=csvReader.get(8);
				stationIdName.put(from, FromName);
				stationIdName.put(to, ToName);
				String time=csvReader.get(1);
				if (!IdOrder.containsKey(from)) {
					IdOrder.put(from, cnt);
					OrderId.put(cnt, from);
					if (cnt==31) {
						System.out.println(cnt+" "+from);
					} else if (cnt==34) {
						System.out.println(cnt+" "+from);
					}
					cnt++;
				}
				if (!IdOrder.containsKey(to)) {
					IdOrder.put(to, cnt);
					OrderId.put(cnt, to);
					if (cnt==31) {
						System.out.println(cnt+" "+from);
					} else if (cnt==34) {
						System.out.println(cnt+" "+from);
					}
					cnt++;
				}
				int slot=getslot(time);
				StationLatitude.put(from, Double.valueOf(csvReader.get(5)));
				StationLatitude.put(to, Double.valueOf(csvReader.get(9)));
				StationLongtitude.put(from, Double.valueOf(csvReader.get(6)));
				StationLongtitude.put(to, Double.valueOf(csvReader.get(10)));
				trajectories.add(new Trajectory(cost, from, to, slot));
			}
			csvReader.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static int getslot(String time) {
		String temp=time.split(" ")[1];
		String[] HMS=temp.split(":");
		int hour=Integer.valueOf(HMS[0]);
		int minute=Integer.valueOf(HMS[1]);
		return hour*2+(minute<30?0:1);
	}
	
	public static void write(){
        String filePath = "query.csv";

        try {
            // 创建CSV写对象
//            CsvWriter csvWriter = new CsvWriter(filePath,',', Charset.forName("GBK"));
            CsvWriter csvWriter = new CsvWriter(filePath);
            // 写表头
            String[] headers = {"query","time","number"};
            csvWriter.writeRecord(headers);
            for (int i=0;i<133333;i++) {
            	Random random=new Random();
            	int number=random.nextInt(7)+2;
            	String[] content = new String[number+3];
            	content[0]="path"+i;
            	int h=random.nextInt(24);
            	int m=random.nextInt(60);
            	int s=random.nextInt(60);
            	String t=h+":"+m+":"+s;
            	content[1]=t;
            	content[2]=String.valueOf(number);
            	for (int j=3;j<content.length;j++) {
            		int x=random.nextInt(330);
            		int id=OrderId.get(x);
            		String name=stationIdName.get(id);
            		content[j]=name;
            	}
                csvWriter.writeRecord(content);
//                jedis.del(content[0]);
//                for (int j=1;j<content.length;j++) {
//                    jedis.rpush(content[0], content[1]);
//                }
            }
            csvWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	public static void main(String[] args) {
		readData();
		write();
	}

}
