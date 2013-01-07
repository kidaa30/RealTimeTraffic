package storm.realTraffic.struct;

import java.util.ArrayList;

public class Polyline {
	public ArrayList<Point> points;
	public Point first;
	public Point last;
	public int count;
	
	public Polyline(){
		this.points = new ArrayList<Point>();
		this.first = null;
		this.last = null;
		this.count = 0;
	}
	public Polyline(ArrayList<Point> points){
		this.points = points;
		this.first = points.get(0);
		this.last = points.get(points.size()-1);
		this.count = points.size();
	}
}
