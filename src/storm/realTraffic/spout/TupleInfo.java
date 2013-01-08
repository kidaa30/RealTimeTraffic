package storm.realTraffic.spout;

import java.util.HashMap;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class TupleInfo {
	
	private String viechleID;
	private String dateTime;
	private String latitude;
	private String longitude;
	private String speed;
	private String melostone;
	private String bearing;
	
	public TupleInfo(){
		
	}

	public Fields getFieldList() {
		// TODO Auto-generated method stub
		Fields fieldList= new Fields(viechleID,dateTime,latitude,
				longitude,speed,melostone,bearing);
//		fieldList[0]=viechleID;
//		fieldList[1]=dateTime;
//		fieldList[2]=Double.toString(lantitude) ;
//		fieldList[3]=Double.toString(longitude) ;
//		fieldList[4]=Integer.toString(speed) ;
//		fieldList[5]=Double.toString(melostone);
//		fieldList[6]=Integer.toString(bearing);
		
		return fieldList;
	}




	public String getDelimiter() {
		// TODO Auto-generated method stub
		String delimiter="|";
		return delimiter;
	
	}


}
