package org.apache.storm.Storm_Experiment;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.DoubleMatrix3D;
import cern.colt.matrix.impl.DenseDoubleMatrix1D;
import cern.colt.matrix.impl.DenseDoubleMatrix2D;
import cern.colt.matrix.impl.DenseDoubleMatrix3D;
import cern.colt.matrix.linalg.Algebra;
import data.Trajectory;

public class ComputeMethod {

	public static DenseDoubleMatrix2D Random2DimensionMartix(int m, int n) {
		double[][] randomMatrix = new double[m][n];
		Random rand = new Random();
		rand.setSeed(System.currentTimeMillis());
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				randomMatrix[i][j] = Math.random();
			}
		}
		return new DenseDoubleMatrix2D(randomMatrix);
	}

	public static DenseDoubleMatrix3D Random3DimensionMartix(int k, int m, int n) {
		double[][][] randomMatrix = new double[k][m][n];
		Random rand = new Random();
		rand.setSeed(System.currentTimeMillis());
		for (int l = 0; l < k; l++) {
			for (int i = 0; i < m; i++) {
				for (int j = 0; j < n; j++) {
					randomMatrix[l][i][j] = Math.random();
				}
			}
		}
		return new DenseDoubleMatrix3D(randomMatrix);
	}

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

	public static DenseDoubleMatrix2D trainDistanceTime(List<Trajectory> trajectories,
			Map<Integer, Double> StationLatitude, Map<Integer, Double> StationLongtitude,
			Map<Integer, Integer> IdOrder) {
		int n = trajectories.size();
		double[] x = new double[n];
		double[] y = new double[n];
		for (int i = 0; i < n; i++) {
			double distance = computeDistance(StationLatitude.get(trajectories.get(i).from),
					StationLongtitude.get(trajectories.get(i).from), StationLatitude.get(trajectories.get(i).to),
					StationLongtitude.get(trajectories.get(i).to));
			x[i] = distance;
			y[i] = trajectories.get(i).cost;
		}
		DenseDoubleMatrix1D X = new DenseDoubleMatrix1D(x);
		DenseDoubleMatrix1D Y = new DenseDoubleMatrix1D(y);
		double theta = (1 / Algebra.DEFAULT.mult(X, X)) * Algebra.DEFAULT.mult(X, Y);

		double[][] r = new double[IdOrder.size()][IdOrder.size()];
		for (Map.Entry<Integer, Integer> entry : IdOrder.entrySet()) {
			for (Map.Entry<Integer, Integer> entry2 : IdOrder.entrySet()) {

				double distance = computeDistance(StationLatitude.get(entry.getKey()),
						StationLongtitude.get(entry.getKey()), StationLatitude.get(entry2.getKey()),
						StationLongtitude.get(entry2.getKey()));
				r[entry.getValue()][entry2.getValue()] = distance * theta;
				// System.out.println(entry.getKey()+" "+entry.getValue()+" "+entry2.getKey()+"
				// "+entry2.getValue()+" "+distance*theta);
			}
		}
		DenseDoubleMatrix2D result = new DenseDoubleMatrix2D(r);
		return result;
	}

	public static DoubleMatrix3D constructTensor(List<Trajectory> trajectories, Map<Integer, Integer> IdOrder) {
		DenseDoubleMatrix3D A = new DenseDoubleMatrix3D(48, 330, 330);
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

	public static DenseDoubleMatrix3D MatrixMult3D2D(DoubleMatrix3D M, DoubleMatrix2D N, int dir) {
		// dir==0 dm*dn*dk m*dm
		// dir==1 n*dn
		// dir==2 k*dk
		// DenseDoubleMatrix3D result=null;
		int dk = M.slices();
		int dm = M.rows();
		int dn = M.columns();
		if (dir == 1) {
			int m = N.rows();
			// result=new DenseDoubleMatrix3D(dk, m, dn);
			double[][][] result = new double[dk][m][dn];
			for (int l = 0; l < dk; l++) {
				DoubleMatrix2D temp = Algebra.DEFAULT.mult(N, M.viewSlice(l));
				result[l] = temp.toArray();
			}
			return new DenseDoubleMatrix3D(result);
		} else if (dir == 2) {
			int n = N.rows();
			// result=new DenseDoubleMatrix3D(dk, dm, n);
			double[][][] result = new double[dk][dm][n];
			for (int l = 0; l < dk; l++) {
				DoubleMatrix2D temp = Algebra.DEFAULT.mult(M.viewSlice(l), Algebra.DEFAULT.transpose(N));
				result[l] = temp.toArray();
			}
			return new DenseDoubleMatrix3D(result);
		} else if (dir == 0) {
			int k = N.rows();
			// result=new DenseDoubleMatrix3D(k, dm, dn);
			double[][][] result = new double[k][dm][dn];
			for (int j = 0; j < dn; j++) {
				DoubleMatrix2D temp = Algebra.DEFAULT.mult(N, M.viewColumn(j));
				for (int a = 0; a < temp.rows(); a++) {
					for (int b = 0; b < temp.columns(); b++) {
						result[a][b][j] = temp.getQuick(a, b);
					}
				}
			}
			return new DenseDoubleMatrix3D(result);
		}
		return null;
	}

	public static DoubleMatrix3D TensorDecomposition(DoubleMatrix3D A, DoubleMatrix2D extimate) {
		DoubleMatrix3D S = Random3DimensionMartix(3, 3, 3);// dm dn dk
		DoubleMatrix2D R = Random2DimensionMartix(330, 3);// dm dir==1
		DoubleMatrix2D U = Random2DimensionMartix(330, 3);// dn dir==2
		DoubleMatrix2D T = Random2DimensionMartix(48, 3);// dk dir==0
		
		double rate = 0.0005;
		double penality1 = 0.00005;
		double penality2 = 0.00003;

		DoubleMatrix3D a = new DenseDoubleMatrix3D(48, 330, 330);
		double L0 = 0;

		a = MatrixMult3D2D(MatrixMult3D2D(MatrixMult3D2D(S, T, 0), R, 1), U, 2);
		double L1 = 0.5 * l2norm3DMatrix(MatrixMinus(A, a))
				+ 0.5 * penality1 * Algebra.DEFAULT.normF(MatrixMinus(MatrixAverage3Dto2D(a), extimate))
				+ 0.5 * penality2 * (Algebra.DEFAULT.normF(R) + Algebra.DEFAULT.normF(U) + Algebra.DEFAULT.normF(T)
						+ l2norm3DMatrix(S));

//		int cnt=0;
		
		while (L1 - L0>100) {
			for (int k = 0; k < 48; k++) {
				for (int i = 0; i < 330; i++) {
					for (int j = 0; j < 330; j++) {
						if (A.get(k, i, j) != 0) {
							double y = MatrixMult3D2D(
									MatrixMult3D2D(MatrixMult3D2D(S, getRowofMatrix(T, k), 0), getRowofMatrix(R, i), 1),
									getRowofMatrix(U, j), 2).get(0, 0, 0);
							
							double c=Math.abs(y - A.getQuick(k, i, j));
//							DoubleMatrix2D lastR = R.copy();
//							DoubleMatrix2D lastU = U.copy();
//							DoubleMatrix2D lastT = T.copy();
//
//							DoubleMatrix2D tempT = MatrixMinus(
//									MatrixMultConst(getRowofMatrix(lastT, k), (1 - rate * penality2)),
//									MatrixMultConst(
//											Convert3Dto2D(MatrixMult3D2D(MatrixMult3D2D(S, getRowofMatrix(lastR, i), 1),
//													getRowofMatrix(lastU, j), 2)),
//											rate * c));
//							DoubleMatrix2D tempR = MatrixMinus(
//									MatrixMultConst(getRowofMatrix(lastR, i), (1 - rate * penality2)),
//									MatrixMultConst(
//											Convert3Dto2D(MatrixMult3D2D(MatrixMult3D2D(S, getRowofMatrix(lastU, j), 2),
//													getRowofMatrix(lastT, k), 0)),
//											rate * c));
//							DoubleMatrix2D tempU = MatrixMinus(
//									MatrixMultConst(getRowofMatrix(lastU, j), (1 - rate * penality2)),
//									MatrixMultConst(
//											Convert3Dto2D(MatrixMult3D2D(MatrixMult3D2D(S, getRowofMatrix(lastR, i), 1),
//													getRowofMatrix(lastT, k), 0)),
//											rate * c));
							
							DoubleMatrix2D tempT = MatrixMinus(
									MatrixMultConst(getRowofMatrix(T, k), (1 - rate * penality2)),
									MatrixMultConst(
											Convert3Dto2D(MatrixMult3D2D(MatrixMult3D2D(S, getRowofMatrix(R, i), 1),
													getRowofMatrix(U, j), 2)),
											rate * c));
							T = setRowofMatrix(T, k, tempT);
							DoubleMatrix2D tempR = MatrixMinus(
							MatrixMultConst(getRowofMatrix(R, i), (1 - rate * penality2)),
							MatrixMultConst(
									Convert3Dto2D(MatrixMult3D2D(MatrixMult3D2D(S, getRowofMatrix(U, j), 2),
											getRowofMatrix(T, k), 0)),
									rate * c));
							R = setRowofMatrix(R, i, tempR);
							DoubleMatrix2D tempU = MatrixMinus(
									MatrixMultConst(getRowofMatrix(U, j), (1 - rate * penality2)),
									MatrixMultConst(
											Convert3Dto2D(MatrixMult3D2D(MatrixMult3D2D(S, getRowofMatrix(R, i), 1),
													getRowofMatrix(T, k), 0)),
											rate * c));
							U = setRowofMatrix(U, j, tempU);

							
							
//							System.out.println(S);
//							System.out.println(A.get(k, i, j));
//							System.out.println(tempT);
//
//							System.out.println(k + " " + i + " " + j);

//							R = setRowofMatrix(R, i, tempR);
//							U = setRowofMatrix(U, j, tempU);
//							T = setRowofMatrix(T, k, tempT);
							S = MatrixMinus(MatrixMultConst(S, (1 - rate * penality2)),
									MatrixMultConst(Korn(tempR, tempU, tempT), rate * c));
							
					}
				}
			}
			}
			L0 = L1;
			a=MatrixMult3D2D(MatrixMult3D2D(MatrixMult3D2D(S, T, 0),R,1),U,2);
			L1=0.5*l2norm3DMatrix(MatrixMinus(A, a))+0.5*penality1*Algebra.DEFAULT.normF(MatrixMinus(MatrixAverage3Dto2D(a), extimate))+
					0.5*penality2*(Algebra.DEFAULT.normF(R)+Algebra.DEFAULT.normF(U)+Algebra.DEFAULT.normF(T)+l2norm3DMatrix(S));
		}
		return a;
	}

	public static DoubleMatrix3D Korn(DoubleMatrix2D M, DoubleMatrix2D N, DoubleMatrix2D K) {
		DoubleMatrix3D SS = new DenseDoubleMatrix3D(K.columns(), M.columns(), N.columns());
		for (int l = 0; l < K.columns(); l++) {
			for (int i = 0; i < M.columns(); i++) {
				for (int j = 0; j < N.columns(); j++) {
					SS.setQuick(l, i, j, M.getQuick(0, i) * N.getQuick(0, j) * K.getQuick(0, l));
				}
			}
		}
		return SS;
	}

	public static DoubleMatrix2D MatrixMinus(DoubleMatrix2D m, DoubleMatrix2D n) {
		DenseDoubleMatrix2D result = new DenseDoubleMatrix2D(m.rows(), m.columns());
		for (int i = 0; i < result.rows(); i++) {
			for (int j = 0; j < result.columns(); j++) {
				result.set(i, j, m.getQuick(i, j) - n.getQuick(i, j));
			}
		}
		return result;
	}

	public static DoubleMatrix3D MatrixMinus(DoubleMatrix3D m, DoubleMatrix3D n) {
		DenseDoubleMatrix3D result = new DenseDoubleMatrix3D(m.slices(), m.rows(), m.columns());
		for (int l = 0; l < result.slices(); l++) {
			for (int i = 0; i < result.rows(); i++) {
				for (int j = 0; j < result.columns(); j++) {
					result.set(l, i, j, m.getQuick(l, i, j) - n.getQuick(l, i, j));
				}
			}
		}
		return result;
	}

	public static DoubleMatrix2D MatrixMultConst(DoubleMatrix2D m, double d) {
		DoubleMatrix2D result = m.like();
		for (int i = 0; i < m.rows(); i++) {
			for (int j = 0; j < m.columns(); j++) {
				result.set(i, j, d * m.getQuick(i, j));
			}
		}
		return result;
	}

	public static DoubleMatrix3D MatrixMultConst(DoubleMatrix3D m, double d) {
		DoubleMatrix3D result = m.like();
		for (int l = 0; l < m.slices(); l++) {
			for (int i = 0; i < m.rows(); i++) {
				for (int j = 0; j < m.columns(); j++) {
					result.set(l, i, j, d * m.getQuick(l, i, j));
				}
			}
		}
		return result;
	}

	public static DoubleMatrix2D Convert3Dto2D(DoubleMatrix3D m) {
		int slice = m.slices();
		int row = m.rows();
		int column = m.columns();
		if (slice == 1) {
			return m.viewSlice(0);
		} else if (row == 1) {
			return m.viewRow(0);
		} else if (column == 1) {
			return m.viewColumn(0);
		}
		return null;
	}

	public static DoubleMatrix2D setRowofMatrix(DoubleMatrix2D m, int row, DoubleMatrix2D n) {
		DoubleMatrix2D result = m.copy();
		for (int i = 0; i < m.columns(); i++) {
			result.set(row, i, n.getQuick(0, i));
		}
		return result;
	}

	public static DoubleMatrix2D getRowofMatrix(DoubleMatrix2D m, int row) {
		double[][] x = m.toArray();
		double[][] y = new double[1][x[0].length];
		y[0] = x[row];
		DoubleMatrix2D xx = new DenseDoubleMatrix2D(y);
		return xx;
	}

	public static double l2norm3DMatrix(DoubleMatrix3D doubleMatrix3D) {
		int k = doubleMatrix3D.slices();
		int m = doubleMatrix3D.rows();
		int n = doubleMatrix3D.columns();
		double sum = 0;
		for (int l = 0; l < k; l++) {
			for (int i = 0; i < m; i++) {
				for (int j = 0; j < n; j++) {
					sum += doubleMatrix3D.getQuick(l, i, j) * doubleMatrix3D.getQuick(l, i, j);
				}
			}
		}
		return Math.sqrt(sum);
	}

	public static DoubleMatrix2D MatrixAverage3Dto2D(DoubleMatrix3D m) {
		DoubleMatrix2D result = new DenseDoubleMatrix2D(m.rows(), m.columns());
		for (int l = 0; l < m.slices(); l++) {
			for (int i = 0; i < m.rows(); i++) {
				for (int j = 0; j < m.columns(); j++) {
					result.setQuick(i, j, (result.get(i, j) * l + m.getQuick(l, i, j) / (l + 1)));
				}
			}
		}
		return result;
	}

	
	
	private static int knn_k=5;
	private static int par1=33;
	private static int par2=17;
	private static int par3=19;
	private static int par4=33;


	
	public static DoubleMatrix3D KNNmodel(DoubleMatrix3D A, Map<Integer, Double> StationLatitude,Map<Integer, Double> StationLongtitude,Map<Integer, Integer> IdOrder,Map<Integer, Integer> OrderId) {
		DoubleMatrix3D result=A.copy();
		for (int k=0;k<result.slices();k++) {
			for (int i=0;i<result.rows();i++) {
				for (int j=0;j<result.columns();j++) {
					if (result.getQuick(k, i, j)==0) {
						System.out.println(k+" "+i+" "+j);
						double knnCalulate=KNNcalcute(A, k, i, j, StationLatitude, StationLongtitude, IdOrder, OrderId);
						result.set(k, i, j, knnCalulate);
					}
				}
			}
		}
		return result;
	}
	
	public static double KNNcalcute(DoubleMatrix3D A,int k,int i,int j, Map<Integer, Double> StationLatitude,Map<Integer, Double> StationLongtitude,Map<Integer, Integer> IdOrder,Map<Integer, Integer> OrderId) {
		double FromLongtitude=StationLongtitude.get(OrderId.get(i));
		double ToLongtitude=StationLongtitude.get(OrderId.get(j));
		double FromLatitude=StationLatitude.get(OrderId.get(i));
		double ToLatitude=StationLatitude.get(OrderId.get(j));
		double distance=computeDistance(FromLatitude, FromLongtitude, ToLatitude, ToLongtitude);
		Map<Double, Double> map=new TreeMap<Double, Double>();
		
		int k1=(k+47)%48;
		int k2=(k+1)%48;
		for (int a=0;a<330;a++) {
			for (int b=0;b<330;b++) {
				if (A.get(k, a, b)!=0) {						
					double FromLongtitudetemp=StationLongtitude.get(OrderId.get(i));
					double ToLongtitudetemp=StationLongtitude.get(OrderId.get(j));
					double FromLatitudetemp=StationLatitude.get(OrderId.get(i));
					double ToLatitudetemp=StationLatitude.get(OrderId.get(j));
					double distancetemp=computeDistance(FromLatitude, FromLongtitude, ToLatitude, ToLongtitude);
					double KNNdistance=kc(distance-distancetemp,FromLatitude-FromLatitudetemp,ToLatitude-ToLatitudetemp,FromLongtitude-FromLongtitudetemp,ToLatitude-ToLongtitudetemp);
					map.put(KNNdistance, A.get(k, a, b));
				}
				if (A.get(k1, a, b)!=0) {
					double FromLongtitudetemp=StationLongtitude.get(OrderId.get(i));
					double ToLongtitudetemp=StationLongtitude.get(OrderId.get(j));
					double FromLatitudetemp=StationLatitude.get(OrderId.get(i));
					double ToLatitudetemp=StationLatitude.get(OrderId.get(j));
					double distancetemp=computeDistance(FromLatitude, FromLongtitude, ToLatitude, ToLongtitude);
					double KNNdistance=kc(distance-distancetemp,FromLatitude-FromLatitudetemp,ToLatitude-ToLatitudetemp,FromLongtitude-FromLongtitudetemp,ToLatitude-ToLongtitudetemp);
					map.put(KNNdistance, A.get(k1, a, b));
				}
				if (A.get(k2, a, b)!=0) {
					double FromLongtitudetemp=StationLongtitude.get(OrderId.get(i));
					double ToLongtitudetemp=StationLongtitude.get(OrderId.get(j));
					double FromLatitudetemp=StationLatitude.get(OrderId.get(i));
					double ToLatitudetemp=StationLatitude.get(OrderId.get(j));
					double distancetemp=computeDistance(FromLatitude, FromLongtitude, ToLatitude, ToLongtitude);
					double KNNdistance=kc(distance-distancetemp,FromLatitude-FromLatitudetemp,ToLatitude-ToLatitudetemp,FromLongtitude-FromLongtitudetemp,ToLatitude-ToLongtitudetemp);
					map.put(KNNdistance, A.get(k2, a, b));
				}
			}
		}
		
		int cnt=0;
		double sum=0;
		for (double x:map.values()) {
			if (cnt++<knn_k) {
				break;
			}
			sum+=x;
		}
		return sum/(cnt+1);
	}
	
	public static double kc(double distancecha,double Lacha1,double Lacha2,double longcha1,double longcha2) {
		return Math.abs(distancecha)/1000+Math.abs(Lacha1)*par1+Math.abs(longcha1)*par2+Math.abs(Lacha2)*par3+Math.abs(longcha2)*par4;
	}
}