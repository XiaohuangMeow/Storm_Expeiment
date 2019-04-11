package data;

import java.io.Serializable;
import java.util.Comparator;

public class Point implements Comparable<Point>,Serializable {
	
	/**
	 * 
	 */
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

    private static final Comparator<Point> p1lat_COMPARATOR = new Comparator<Point>() {
        public int compare(Point o1, Point o2) {
        	if (o1.p1.latitude<o2.p1.latitude) {
        		return -1;
        	}
        	if (o1.p1.latitude>o2.p1.latitude) {
        		return 1;
        	}
        	return 0;
        }
    };

    private static final Comparator<Point> p1long_COMPARATOR = new Comparator<Point>() {
    	public int compare(Point o1, Point o2) {
        	if (o1.p1.longitude<o2.p1.longitude) {
        		return -1;
        	}
        	if (o1.p1.longitude>o2.p1.longitude) {
        		return 1;
        	}
        	return 0;
        }
    };
    
    private static final Comparator<Point> p2lat_COMPARATOR = new Comparator<Point>() {
        public int compare(Point o1, Point o2) {
        	if (o1.p2.latitude<o2.p2.latitude) {
        		return -1;
        	}
        	if (o1.p2.latitude>o2.p2.latitude) {
        		return 1;
        	}
        	return 0;
        }
    };

    private static final Comparator<Point> p2long_COMPARATOR = new Comparator<Point>() {
    	public int compare(Point o1, Point o2) {
        	if (o1.p2.longitude<o2.p2.longitude) {
        		return -1;
        	}
        	if (o1.p2.longitude>o2.p2.longitude) {
        		return 1;
        	}
        	return 0;
        }
    };

    protected static final int p1lat_AXIS = 0;
    protected static final int p1long_AXIS = 1;
    protected static final int p2lat_AXIS = 2;
    protected static final int p2long_AXIS = 3;
	
    protected final double x;
    protected final PlaceInfomation p1;
    protected final PlaceInfomation p2;

    public Point(double x,PlaceInfomation p1, PlaceInfomation p2) {
        this.x=x;
    	this.p1 = p1;
        this.p2 = p2;
    }
    
    public double getx() {
        return x;
    }
    
    public PlaceInfomation getp1() {
        return p1;
    }
    
    public PlaceInfomation getp2() {
        return p2;
    }
    
    public double roadDistance() {
    	return computeDistance(p1.latitude, p1.longitude, p2.latitude, p2.longitude);
    }

    public double myDistance(Point o1) {
        return myDistance(o1, this);
    }

    private final double myDistance(Point o1, Point o2) {
        return Math.sqrt(Math.pow((o1.p1.latitude - o2.p1.latitude), 2) + Math.pow((o1.p1.longitude - o2.p1.longitude), 2) 
        				+ Math.pow((o1.p2.latitude - o2.p2.latitude), 2) + Math.pow((o1.p2.longitude - o2.p2.longitude), 2));
    }

    public int compareTo(Point o) {
        int p1lat_Comp = p1lat_COMPARATOR.compare(this, o);
        if (p1lat_Comp != 0)
            return p1lat_Comp;
        int p1long_Comp = p1long_COMPARATOR.compare(this, o);
        if (p1long_Comp != 0)
            return p1long_Comp;
        int p2lat_Comp = p2lat_COMPARATOR.compare(this, o);
        if (p2lat_Comp != 0)
            return p2lat_Comp;
        int p2long_Comp = p2long_COMPARATOR.compare(this, o);
        return p2long_Comp;
    }


}
