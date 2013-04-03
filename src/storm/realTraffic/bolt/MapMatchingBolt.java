
/**
 * realODMatrix realODMatrix.bolt DistrictMatchingBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Author: admin
 * Last Updated:2013-1-8 锟斤拷锟斤拷2:39:14
 * email: gh.chen@siat.ac.cn
 */
package storm.realTraffic.bolt;
import java.io.IOException;
import java.util.List;
import java.util.Map;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


import backtype.storm.tuple.Tuple;
import storm.realTraffic.gis.*;
import storm.realTraffic.spout.FieldListenerSpout;

/**
 * realODMatrix realODMatrix.bolt DistrictMatchingBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 2:39:14
 * email: gh.chen@siat.ac.cn 
 */
public class MapMatchingBolt implements IRichBolt {

	private static final long serialVersionUID = -433427751113113358L;

	//private static final long serialVersionUID = 1L;
	private OutputCollector _collector;

	Integer roadID ;
	GPSRcrd record;
	Map<GPSRcrd, Integer> gpsMatch;  //map<GPSRcrd,roadID>
	Integer taskID;
	String taskname;
	List<Object> inputLine; 
	Fields matchBoltDeclare=null;

	static String path = "/home/ghchen/sects/szRoads/SZRoads.shp";
	static roadgridList sects=null ;	
	static int count=0;




	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector=collector;	
		this.taskID=context.getThisTaskId();
		this.taskname=context.getThisComponentId();

	}


	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		//String path = "E:/datasource/sztb/dat/base/sects/Sects.shp";

		try {
			if(sects==null){
			sects= new roadgridList(path);
			}
			//System.out.println("District Match input:"+input.toString());
			//FieldListenerSpout.writeToFile("/home/ghchen/output","District Match input:"+input.toString());
 

		  List<Object> inputLine = input.getValues();//getFields();
		  Fields inputLineFields = input.getFields();
	//FieldListenerSpout.writeToFile("/home/ghchen/output","DistrictMap inputLineFields"+inputLineFields);	  


			record=new GPSRcrd(Double.parseDouble((String) inputLine.get(6)), 
					Double.parseDouble((String) inputLine.get(5)), Integer.parseInt((String) inputLine.get(3)), 
					Integer.parseInt((String) inputLine.get(4)));

			if(     Double.parseDouble((String) inputLine.get(6)) > 114.5692938 ||
					Double.parseDouble((String) inputLine.get(6)) < 113.740000  ||
					Double.parseDouble((String) inputLine.get(5)) > 22.839945   ||
					Double.parseDouble((String) inputLine.get(5)) < 22.44
					) return;


			//roadID = sects.fetchSect(record);
			
			roadID = sects.fetchRoadID(record);

			if(roadID!=-1)
			{
				System.out.print("[count:"+count++ +"]: GPS Point falls into Road No. :" + roadID);
				//FieldListenerSpout.writeToFile("/home/ghchen/roadID","DistrictBolt GPS Point falls into Sect No. ::"+roadID.toString());



				inputLine.add(Integer.toString(roadID));			
				//input.getFields().toList().add("roadID");
				List<String> fieldList= input.getFields().toList();
				fieldList.add("roadID");
				matchBoltDeclare=new Fields(fieldList);
				//FieldListenerSpout.writeToFile("/home/ghchen/output","matchBoltDeclare="+matchBoltDeclare);		


				String[] obToStrings=new String[inputLine.size()];
				obToStrings=inputLine.toArray(obToStrings);
				//			for(int i=0;i<obToStrings.length-1;i++)
				//			FieldListenerSpout.writeToFile("/home/ghchen/map-oput",obToStrings[i]+",");
				//			FieldListenerSpout.writeToFile("/home/ghchen/map-oput","\n");


				_collector.emit(new Values(obToStrings));
				//_collector.emit(new Values(inputLine));
			}

		} catch (Exception e) {

			e.printStackTrace();
		}	

		_collector.ack(input);

	}


	public void cleanup() {
		// TODO Auto-generated method stub

		System.out.println("-- District Mathchier ["+taskname+"-"+roadID+"] --");
		for(Map.Entry<GPSRcrd, Integer> entry : gpsMatch.entrySet()){
		System.out.println(entry.getKey()+": "+entry.getValue());
		}

	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields ("viechleID", "dateTime", "occupied", "speed", 
				"bearing", "latitude", "longitude", "roadID"));				
	}


	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}