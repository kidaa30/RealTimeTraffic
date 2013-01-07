package storm.realTraffic.spout;

import java.util.HashMap;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class TupleInfo {
	
	private String viechleID;
	private String dateTime;
	private double lantitude;
	private double longitude;
	private int speed;
	private int melostone;
	private int bearing;
	
	

	public static Fields getFieldList() {
		// TODO Auto-generated method stub
		Fields tupleField=null;
		
		
		
		return tupleField;
	}




	public static String getDelimiter() {
		// TODO Auto-generated method stub
		String delimiter="|";
		return delimiter;
	
	}




}
