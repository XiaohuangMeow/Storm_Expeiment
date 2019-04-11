package data;

public class Trajectory {
	public double cost;
	public int from;
	public int to;
	public int slot;
	
	public Trajectory(double cost, int from, int to, int slot) {
		super();
		this.cost = cost;
		this.from = from;
		this.to = to;
		this.slot = slot;
	}	
}
