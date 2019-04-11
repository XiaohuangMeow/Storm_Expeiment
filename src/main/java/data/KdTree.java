package data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class KdTree implements Serializable{

	private static final long serialVersionUID = 1L;
	private int k = 4;
    private KdNode root = null;

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

    public KdTree(List<Point> list) {
        super();
        root = createNode(list, k, 0);
    }

    public KdTree(List<Point> list, int k) {
        super();
        root = createNode(list, k, 0);
    }
    
    //递归创建
    private static KdNode createNode(List<Point> list, int k, int depth) {
        if (list == null || list.size() == 0)
            return null;

        int axis = depth % k;
        if (axis == p1lat_AXIS)
            Collections.sort(list, p1lat_COMPARATOR);
        else if (axis == p1long_AXIS) 
            Collections.sort(list, p1long_COMPARATOR);
        else if (axis == p2lat_AXIS)
            Collections.sort(list, p2lat_COMPARATOR);
        else if (axis == p2long_AXIS) {
            Collections.sort(list, p2lat_COMPARATOR);
        }

        KdNode node = null;
        List<Point> less = new ArrayList<Point>(list.size());
        List<Point> more = new ArrayList<Point>(list.size());
        if (list.size() > 0) {
            int medianIndex = list.size() / 2;
            node = new KdNode(list.get(medianIndex), k, depth);
            // Process list to see where each non-median point lies
            for (int i = 0; i < list.size(); i++) {
                if (i == medianIndex)
                    continue;
                Point p = list.get(i);
                // Cannot assume points before the median are less since they could be equal
                if (KdNode.compareTo(depth, k, p, node.id) <= 0) {
                    less.add(p);
                } else {
                    more.add(p);
                }
            }

            if ((medianIndex-1 >= 0) && less.size() > 0) {
                node.lesser = createNode(less, k, depth + 1);
                node.lesser.parent = node;
            }

            if ((medianIndex <= list.size()-1) && more.size() > 0) {
                node.greater = createNode(more, k, depth + 1);
                node.greater.parent = node;
            }
        }

        return node;
    }

    /** 
     * Searches the K nearest neighbor.
     *
     * @param K
     *            Number of neighbors to retrieve. Can return more than K, if
     *            last nodes are equal distances.
     * @param value
     *            to find neighbors of.
     * @return Collection of T neighbors.
     */
    @SuppressWarnings("unchecked")
    public List<Point> nearestNeighbourSearch(int K, Point value) {
        if (value == null || root == null)
            return Collections.EMPTY_LIST;

        TreeSet<KdNode> results = new TreeSet<KdNode>(new MydistanceComparator(value));

        // Find the closest leaf node
        KdNode prev = null;
        KdNode node = root;
        while (node != null) {
            if (KdNode.compareTo(node.depth, node.k, value, node.id) <= 0) {
                // Lesser
                prev = node;
                node = node.lesser;
            } else {
                // Greater
                prev = node;
                node = node.greater;
            }
        }
        KdNode leaf = prev;

        if (leaf != null) {
            // Used to not re-examine nodes
            Set<KdNode> examined = new HashSet<KdNode>();

            // Go up the tree, looking for better solutions
            node = leaf;
            while (node != null) {
                // Search node
                searchNode(value, node, K, results, examined);
                node = node.parent;
            }
        }

        // Load up the collection of the results
        List<Point> collection = new ArrayList<Point>(K);
        for (KdNode kdNode : results)
        	collection.add((Point) kdNode.id);
        return collection;
    }

    private static final void searchNode(Point value, KdNode node, int K, TreeSet<KdNode> results, Set<KdNode> examined) {
        examined.add(node);

        // Search node
        KdNode lastNode = null;
        Double lastDistance = Double.MAX_VALUE;
        if (results.size() > 0) {
            lastNode = results.last();
            lastDistance = lastNode.id.myDistance(value);
        }
        Double nodeDistance = node.id.myDistance(value);
        if (nodeDistance.compareTo(lastDistance) < 0) {
            if (results.size() == K && lastNode != null)
                results.remove(lastNode);
            results.add(node);
        } else if (nodeDistance.equals(lastDistance)) {
            results.add(node);
        } else if (results.size() < K) {
            results.add(node);
        }
        
        lastNode = results.last();
        lastDistance = lastNode.id.myDistance(value);

        int axis = node.depth % node.k;
        KdNode lesser = node.lesser;
        KdNode greater = node.greater;

        // Search children branches, if axis aligned distance is less than
        // current distance
        if (lesser != null && !examined.contains(lesser)) {
            examined.add(lesser);

            double nodePoint = Double.MIN_VALUE;
            double valuePlusDistance = Double.MIN_VALUE;
            if (axis == p1lat_AXIS) {
                nodePoint = node.id.p1.latitude;
                valuePlusDistance = value.p1.latitude - lastDistance;
            } else if (axis == p1long_AXIS) {
                nodePoint = node.id.p1.longitude;
                valuePlusDistance = value.p1.longitude - lastDistance;
            } else if (axis == p2lat_AXIS) {
                nodePoint = node.id.p2.latitude;
                valuePlusDistance = value.p2.latitude - lastDistance;
            } else if (axis == p2long_AXIS) {
                nodePoint = node.id.p2.longitude;
                valuePlusDistance = value.p2.longitude - lastDistance;
            } 
            boolean lineIntersectsCube = ((valuePlusDistance <= nodePoint) ? true : false);

            // Continue down lesser branch
            if (lineIntersectsCube)
                searchNode(value, lesser, K, results, examined);
        }
        if (greater != null && !examined.contains(greater)) {
            examined.add(greater);

            double nodePoint = Double.MIN_VALUE;
            double valuePlusDistance = Double.MIN_VALUE;
            if (axis == p1lat_AXIS) {
                nodePoint = node.id.p1.latitude;
                valuePlusDistance = value.p1.latitude + lastDistance;
            } else if (axis == p1long_AXIS) {
                nodePoint = node.id.p1.longitude;
                valuePlusDistance = value.p1.longitude + lastDistance;
            } else if (axis == p2lat_AXIS) {
                nodePoint = node.id.p2.latitude;
                valuePlusDistance = value.p2.latitude + lastDistance;
            } else if (axis == p2long_AXIS) {
                nodePoint = node.id.p2.longitude;
                valuePlusDistance = value.p2.longitude + lastDistance;
            }
            boolean lineIntersectsCube = ((valuePlusDistance >= nodePoint) ? true : false);

            // Continue down greater branch
            if (lineIntersectsCube)
                searchNode(value, greater, K, results, examined);
        }
    }


    protected static class MydistanceComparator implements Comparator<KdNode> {

        private final Point point;

        public MydistanceComparator(Point point) {
            this.point = point;
        }

        public int compare(KdNode o1, KdNode o2) {
            Double d1 = point.myDistance(o1.id);
            Double d2 = point.myDistance(o2.id);
            if (d1.compareTo(d2) < 0)
                return -1;
            else if (d2.compareTo(d1) < 0)
                return 1;
            return o1.id.compareTo(o2.id);
        }
    }

    public static class KdNode implements Comparable<KdNode> , Serializable{

		private static final long serialVersionUID = 1L;
		private final Point id;
        private final int k;
        private final int depth;

        private KdNode parent = null;
        private KdNode lesser = null;
        private KdNode greater = null;

        public KdNode(Point id) {
            this.id = id;
            this.k = 5;
            this.depth = 0;
        }

        public KdNode(Point id, int k, int depth) {
            this.id = id;
            this.k = k;
            this.depth = depth;
        }

        public static int compareTo(int depth, int k, Point o1, Point o2) {
            int axis = depth % k;
            if (axis == p1lat_AXIS)
            	return p1lat_COMPARATOR.compare(o1, o2);
            else if (axis == p1long_AXIS)
            	return p1long_COMPARATOR.compare(o1, o2);
            else if (axis == p2lat_AXIS)
            	return p2lat_COMPARATOR.compare(o1, o2);
//            else if (axis == p2long_AXIS)
            return p2long_COMPARATOR.compare(o1, o2);
        }

        @Override
        public int hashCode() {
            return 31 * (this.k + this.depth + this.id.hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            if (!(obj instanceof KdNode))
                return false;

            KdNode kdNode = (KdNode) obj;
            if (this.compareTo(kdNode) == 0)
                return true;
            return false;
        }

        public int compareTo(KdNode o) {
            return compareTo(depth, k, this.id, o.id);
        }
    }



}