/**
 * realODMatrix realODMatrix.bolt CountBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 閿熸枻鎷烽敓鏂ゆ嫹2:45:05
 * email: gh.chen@siat.ac.cn
 */
package  storm.realTraffic.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.List;

import java.util.Timer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.geotools.feature.visitor.AverageVisitor.AverageResult;


/**
 * realODMatrix realODMatrix.bolt CountBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 2:45:05
 * email: gh.chen@siat.ac.cn
 *
 */
public class SpeedCalculatorBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	double lanLast;   // last location of the vehicle
	double lonLast;
	Date dateTimeLast=null;
	int INTERVAL0 = 120; // We set time windows between two points 120 seconds;
	double DIST0=0.008993;  //  On the Earth, 1 Degree =111.2 km 
	                        //Distance between two points 1km, shoule be 1/111.2 =0.008993 Degree;

	private OutputCollector _collector;	
	Integer taskId;
	String taskName;
	//Map<String, List<String> > Roads; //RoadID, vehicleIdsInThisArea
	public  LinkedList<Road>  Roads = new  LinkedList<Road>();
	//static public List<String> vehicleIdsInThisArea=new ArrayList<String>(); 
	Integer cnt;
	Timer timer;
	
	public class spdList extends ArrayList<Integer>{
		private static final long serialVersionUID = 1L;
		Integer speed;
		spdList(){}
		spdList(Integer speed){this.speed=speed;};
	}

	public class Road 
	{
		public Road(){}
		public Road(String roadID, ArrayList<Integer>  roadSpd) {
			// TODO Auto-generated constructor stub
			this.roadId=roadID;
			this.roadSpd=roadSpd;
			
		}
		public String roadId;
		public int count;//计算次数，是车牌号的个数码
		//public Date dateTime; //该小区统计的车辆出现时间
		ArrayList<Integer> roadSpd;
		int avgSpd;
		//public HashMap<String,spdList> roadSpd; //存放车辆Id的集合,也要把时间存者，以对每一辆车进行计算时间距离
		//public HashMap<String,String> vieLngLatIDList; //存放车辆Id的集合,也要把时间存者，以对每一辆车进行计算时间距离
	}

	public  Road  getRoadById(String RoadId){
		for(Road d : Roads){
			if(d.roadId.equals(RoadId)){
				return d;
			}
		}
		return null;
	}

	/*public  String getlngLatByViecheId(String RoadId,String viechId){
		for(Road d : Roads){
			if(d.RoadId.equals(RoadId)){
				return  d.vieLngLatIDList.get(viechId);
			}
		}
		return null;
	}

	public  void setlngLatByViecheId(String RoadId,String viechId,String lngLat){
		for(Road d : Roads){
			if(d.RoadId.equals(RoadId)){
			   d.vieLngLatIDList.put(viechId, lngLat);
			}
		}
	}

	public  Date getDateByViecheId(String RoadId,String viechId){
		for(Road d : Roads){
			if(d.RoadId.equals(RoadId)){
				return  d.RoadSpd.get(viechId);
			}
		}
		return null;
	}

	public  void setDateByViecheId(String RoadId,String viechId,Date dateTime){
		for(Road d : Roads){
			if(d.RoadId.equals(RoadId)){
			   d.RoadSpd.put(viechId, dateTime);
			}
		}
	}*/


    public  Boolean isDisExits(List<Road>  Roads,  String RoadId){
    	for(Road d : Roads){
			if(d.roadId.equals(RoadId)){
				return true;
			}
		}
    	return false;
    }



	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.taskName = context.getThisComponentId();
		this.taskId = context.getThisTaskId();
		this._collector = collector;		
	}


	@SuppressWarnings("null")
	@Override
	public void execute(Tuple input) {

		String RoadID = input.getValues().get(7).toString();
		double lan = Double.parseDouble(input.getValues().get(5).toString());// lan
		double lon = Double.parseDouble(input.getValues().get(6).toString()); //lon
		String viechId = input.getValues().get(0).toString();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Integer speed=Integer.parseInt(input.getValues().get(3).toString());
		Date dateTime = null;
		try {
			dateTime = sdf.parse(input.getValues().get(1).toString());

		} catch (ParseException e1) {
			e1.printStackTrace();
		}

		if (!isDisExits(Roads, RoadID)) {
			 //没有此小区，则新建一个路径，并存起来				
			//System.out.println("RoadID:"+RoadID+"dateTime:"+dateTime+"viechId"+viechId);
			ArrayList<Integer> roadSpd = new ArrayList<Integer>() ; 
			roadSpd.add(speed);
			Road road = new Road();
			road.roadId = RoadID;		
			road.count = 1;
			road.roadSpd=roadSpd;
			
			road.avgSpd=-1;
			
			Roads.add(road);  //添加路线
			return ;

		}else{   //如果已经有该小区
			Road road=getRoadById(RoadID);
			//if(!Road.roadSpd.contains(viechId)){  //但是如果车辆ID是第一次进入该区域，新建一个车辆ID，并保存；


			int sum=0;
			double avg=-1;

			if(road.roadSpd.size()>=8){
				for(int i=0;i<road.roadSpd.size();i++){
					sum=sum+road.roadSpd.get(i);				 
				} 
				avg=(double)sum/road.roadSpd.size();

				double temp=0;
				for(int i=0;i<road.roadSpd.size();i++)
				{
					temp+=Math.pow((road.roadSpd.get(i)-avg), 2);
				}
				temp = temp/(road.roadSpd.size()-1);
				double standdev =  Math.sqrt(temp);
				if(  Math.abs(speed-avg) <=2* standdev  )
				{
					road.count++;
					road.roadSpd.add(speed);	
					road.avgSpd=(int) avg;
				}

			}
				
								
			/*}else{ //否则，这辆车是多次出现在该区域，则判断这个车辆ID 和上一次出现的时间间隔和距离
				String lngLat = getlngLatByViecheId(RoadID,viechId);
				String[]  s = lngLat.split("_");
				lonLast = Double.parseDouble(s[0]);
				lanLast = Double.parseDouble(s[1]);

				long interval = 0;
				dateTimeLast = getDateByViecheId(RoadID, viechId);
				interval = (dateTime.getTime() - dateTimeLast.getTime()) / 1000;
				double dist = Math.sqrt(Math.pow(lan - lanLast, 2) + Math.pow(lon - lonLast, 2));

				if (dist > DIST0 && interval > INTERVAL0) {					

					Road.count ++;
					Road.dateTime = dateTime;
					Road.RoadSpd.put(viechId, dateTime);
					Road.vieLngLatIDList.put(viechId, lon+"_"+lan);	
				}else{
					return;					
				}
			}*/
		}


		Date nowDate=new Date();
		SimpleDateFormat sdf2= new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		SimpleDateFormat sdf3= new SimpleDateFormat("yyyy-MM-dd");
		int min=nowDate.getMinutes();
		int second=nowDate.getSeconds();
		if( (min%5) ==0 && (second==0) ){
			String nowTime=sdf2.format(nowDate);


			LinkedList<Road> d=new  LinkedList<Road> (Roads);
			Roads.clear();

			 String cur_dir=System.getProperty("user.dir");
			 cur_dir=cur_dir+"/"+sdf3.format(nowDate);
			 newFolder(cur_dir);

			 cur_dir=cur_dir+"/"+"vehicleList-"+nowTime;

			SpeedCalculatorBolt.writeToFile(cur_dir,d);

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

//		timer=new Timer(true);
//		TimerTask Job= new TimerTask() {		
//			@Override
//			public void run() {
//				SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
//				String nowtime=sdf.format(new Date());
//				CountBolt.writeToFile("vehicleList-"+nowtime,Roads);
//			}
//		};
//		timer.schedule(Job,0, 60*1000);  //every 600 seconds.


		_collector.ack(input);

	}


	@Override
	public void cleanup() {
		System.out.println("-- Word Counter ["+taskName+"-"+taskId+"] --");
//		for(Map.Entry<GPSRcrd, Integer> entry : gpsMatch.entrySet()){
//		System.out.println(entry.getKey()+": "+entry.getValue());
//		}

	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("Roads"));
	}


	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

    static class Job extends java.util.TimerTask{   
        @Override  
        public void run() {   
            // TODO Auto-generated method stub  
         
        }  
    } 
    
	public static void writeToFile(String fileName, LinkedList<Road> Roads){
		try {
              BufferedWriter br = new BufferedWriter(new FileWriter(fileName,true));
     		  SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//				String nowtime=sdf.format(new Date());
     		  // ddRoad=Roads;
              for(Road d:Roads){
//            	  br.write(d.RoadId+","+d.count+"#"+d.RoadSpd.values()+";"+
//                    d.vieLngLatIDList.values()+"\n"); 
            	  br.write(d.roadId+","+d.count+","+d.avgSpd);
            	           	  
/*          		for(Map.Entry<String,Date> entry : d.roadSpd.entrySet()){   //
          			String lonLanString=d.vieLngLatIDList.get(entry.getKey()); 
          			//if(entry.getKey()!=null && entry.getValue()!=null && lonLanString!=null)
          			br.write(entry.getKey()+","+sdf.format(entry.getValue()) +","+lonLanString+";");
         			System.out.println(entry.getKey()+","+entry.getValue()+","+lonLanString+";");
          			}*/
          		
          		br.write("\r\n");

          		//System.out.println("\n");
              }         
           
              /*for(Road d : Roads){
               	  br.write(d.RoadId + ","+ d.count + "#");
            	  HashMap<String ,String> viechIds = d.vieLngLatIDList;
            	  Set<String> set = viechIds.keySet();
            	  Iterator<String> iterator = set.iterator();
            	  while(iterator.hasNext()){
            		  String id = iterator.next();
            		  br.write(id+"    ");
            	  }
              }*/
		      br.flush();
		      br.close();		      
        	  Roads.clear();				
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
	}

	 public static void newFolder(String folderPath) { 
		    try { 
		      String filePath = folderPath.toString(); 
		      //filePath = filePath.toString(); 
		      java.io.File myFilePath = new java.io.File(filePath); 
		      if (!myFilePath.exists()) { 
		        myFilePath.mkdir(); 
		      } 
		    } 
		    catch (Exception e) { 
		      System.out.println("Eorror: Can't create new folder!"); 
		      e.printStackTrace(); 
		    } 
		  }

}