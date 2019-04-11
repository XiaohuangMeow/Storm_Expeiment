package data;

import java.io.Serializable;

public class PlaceInfomation implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String name;
	public double latitude;
	public double longitude;
	public int placeid;
	
	public PlaceInfomation(String name, double latitude, double longitude, int placeid) {
		super();
		this.name = name;
		this.latitude = latitude;
		this.longitude = longitude;
		this.placeid = placeid;
	}

}
