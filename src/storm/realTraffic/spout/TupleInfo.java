package storm.realTraffic.spout;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class TupleInfo {
	
	private String viechleID;
	private String dateTime;
	private Double latitude;
	private Double longitude;
	private Integer speed;
	private Double melostone;
	private Integer bearing;
	private int numMember=7;
	
	
	public TupleInfo(){
		
	}
	
	public TupleInfo(List<String> input){
		int lstLength=input.size();
		if(numMember!=input.size())
			System.out.print("input list size mismatch");// ("input list size mismatch");
		else 
		{
			 viechleID=input.get(0);	
			 dateTime=input.get(1);
			 latitude=Double.parseDouble(input.get(2));
			 longitude=Double.parseDouble(input.get(3));
			 speed=Integer.parseInt(input.get(4));
			 melostone=Double.parseDouble(input.get(5));
			 bearing=Integer.parseInt(input.get(6));			
		}
	}
	


/*	public String[] getFieldList() {
		// TODO Auto-generated method stub
		//Fields fieldList= new Fields(viechleID,dateTime,latitude,longitude,speed,melostone,bearing);
		String[] fieldList= new String[numMember];

		
			
		fieldList[0]=viechleID;
		fieldList[1]=dateTime;
		fieldList[2]=Double.toString(latitude) ;
		fieldList[3]=Double.toString(longitude) ;
		fieldList[4]=Integer.toString(speed) ;
		fieldList[5]=Double.toString(melostone);
		fieldList[6]=Integer.toString(bearing);
		
		return fieldList;
	}*/
	
	
	public Fields getFieldList() {		
		//Fields fieldList= new Fields(viechleID,dateTime,latitude,longitude,speed,melostone,bearing);
		Fields fieldList= new Fields (viechleID,dateTime,Double.toString(latitude),
				Double.toString(longitude),Integer.toString(speed),Double.toString(melostone),
				Integer.toString(bearing));		
		return fieldList;
	}




	public String getDelimiter() {
		// TODO Auto-generated method stub
		String delimiter="|";
		return delimiter;
	
	}


}
