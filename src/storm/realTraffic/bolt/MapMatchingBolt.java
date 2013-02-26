
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

	Integer districtID ;
	GPSRcrd record;
	Map<GPSRcrd, Integer> gpsMatch;  //map<GPSRcrd,districtID>
	Integer taskID;
	String taskname;
	List<Object> inputLine; 
	Fields matchBoltDeclare=null;

	static String path = "/home/ghchen/sects/sects.shp";
	static roadSects sects=null ;	
	int count=0;




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
			sects= new roadSects(path);
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


			districtID = sects.fetchSect(record);

			if(districtID!=-1)
			{
				System.out.println(count++ +": GPS Point falls into Sect No. :" + districtID);
				//FieldListenerSpout.writeToFile("/home/ghchen/districtID","DistrictBolt GPS Point falls into Sect No. ::"+districtID.toString());



				inputLine.add(Integer.toString(districtID));			
				//input.getFields().toList().add("districtID");
				List<String> fieldList= input.getFields().toList();
				fieldList.add("districtID");
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

		System.out.println("-- District Mathchier ["+taskname+"-"+districtID+"] --");
		for(Map.Entry<GPSRcrd, Integer> entry : gpsMatch.entrySet()){
		System.out.println(entry.getKey()+": "+entry.getValue());
		}

	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields ("viechleID", "dateTime", "occupied", "speed", 
				"bearing", "latitude", "longitude", "districtID"));				
	}


	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}