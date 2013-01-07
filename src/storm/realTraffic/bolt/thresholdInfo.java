package storm.realTraffic.bolt;

import java.io.Serializable;
import storm.realTraffic.spout.tupleInfo;

public class thresholdInfo extends tupleInfo implements Serializable{
    /**
	 * 
	 */
	//private static final long serialVersionUID = 1L;
	private String action;
    private String rule;
    private static Object thresholdValue;
    private static int thresholdColNumber;
    private static Integer timeWindow;
    private int frequencyOfOccurence;
    
	public static int getThresholdColNumber() {
		// TODO Auto-generated method stub
		return thresholdColNumber;
	}
	public static Object getThresholdValue() {
		// TODO Auto-generated method stub
		return thresholdValue;
	}
	public static int getFrequencyOfOccurence() {
		// TODO Auto-generated method stub
		return 0;
	}
	public static String getAction() {
		// TODO Auto-generated method stub
		return null;
	}
	public static Integer getTimeWindow() {
		// TODO Auto-generated method stub
		return timeWindow;
	}  

}
