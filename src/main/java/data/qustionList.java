package data;

import java.io.Serializable;
import java.util.List;

public class qustionList implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public int seq;
	public List<Integer> q;
//	public DoubleMatrix3D A;
	public int queryslot;
//	public List<PlaceInfomation> places;
	public qustionList(int seq, List<Integer> q , int queryslot) {
		super();
		this.seq = seq;
		this.q = q;
		this.queryslot = queryslot;
	}
	
	
}
