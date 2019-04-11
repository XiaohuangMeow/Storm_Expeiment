package data;

import java.io.Serializable;

public class DistanceTime implements Serializable{
	
	private static final long serialVersionUID = 1L;
	public double distance;
	public double time;
	
	public DistanceTime(double distance, double time) {
		super();
		this.distance = distance;
		this.time = time;
	}
	
	
}
