package scs.util.repository;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import scs.pojo.LoaderDriver;
import scs.pojo.QueryData;
import scs.pojo.ThreeTuple;
import scs.util.loadGen.driver.example.ExampleDriver;
import scs.util.loadGen.driver.serverless.FuncGraphServiceDriver;
import scs.util.rmi.RmiService;

/**
 * System static repository class
 * Provide memory storage in the form of static variables for data needed in system operation
 * Including some system parameters, application run data, control signs and so on
 * @author Yanan Yang
 *
 */
public class Repository{ 
	private static Repository repository=null;
	private Repository(){}
	public synchronized static Repository getInstance() {
		if (repository == null) {
			repository = new Repository();
		}
		return repository;
	}  

	public static int NUMBER_LC=1; //number of LC services 

	public static int windowSize=60; //window size of latency recorder
	public static int recordInterval=1000; //record interval of latency recorder
	private static boolean rmiServiceEnable=false;

	//系统控制变量
	public static boolean[] onlineQueryThreadRunning=new boolean[NUMBER_LC]; 
	public static boolean[] onlineDataFlag=new boolean[NUMBER_LC]; 
	public static boolean[] sendFlag=new boolean[NUMBER_LC]; 

	//负载发生控制变量
	public static int[] realRequestIntensity=new int[NUMBER_LC]; 
	public static int[] realQueryIntensity=new int[NUMBER_LC];  
	private static int[] windowOnLineDataListCount=new int[NUMBER_LC];	
	public static int[] statisticsCount=new int[NUMBER_LC];	
	public static int[] totalRequestCount=new int[NUMBER_LC];
	public static int[] totalQueryCount=new int[NUMBER_LC];

	public static int[] concurrency=new int[NUMBER_LC];

	//负载数据存储变量
	public static List<ArrayList<Integer>> onlineDataList=new ArrayList<ArrayList<Integer>>();
	public static List<ArrayList<ThreeTuple<Integer,String,Timestamp>>> onlineDataListSpec=new ArrayList<ArrayList<ThreeTuple<Integer,String,Timestamp>>>();// <latency,html,collectTime>
	public static List<ArrayList<Integer>> tempOnlineDataList=new ArrayList<ArrayList<Integer>>();
	public static List<ArrayList<QueryData>> windowOnlineDataList=new ArrayList<ArrayList<QueryData>>();
	private static List<ArrayList<QueryData>> tempWindowOnlineDataList=new ArrayList<ArrayList<QueryData>>();

	//统计变量
	public static QueryData[] latestOnlineData=new QueryData[NUMBER_LC];
	public static float[] windowAvgPerSec99thQueryTime=new float[NUMBER_LC];
	public static float[] windowAvgPerSecAvgQueryTime=new float[NUMBER_LC];

	//RMI服务变量
	public static String serverIp="";
	public static int rmiPort;

	//服务驱动注册变量
	public static Map<Integer,LoaderDriver> loaderMap=new HashMap<Integer,LoaderDriver>(); 
	private static List<ThreeTuple<String,String,String>> serverDriverList=new ArrayList<>();

	public static String webServiceNamesStr="";
	public static String webServiceURLsStr="";
	public static String webServiceParamsStr="";
	public static String exampleURL="";

	//static code
	static {
		readProperties();
		initServiceDriver();
		initList();
		initLoaderMap();

		if(Repository.rmiServiceEnable==true)
			RmiService.getInstance().service(Repository.serverIp, Repository.rmiPort);//start the RMI service
	}
	/**
	 * read properties 
	 */
	private static void readProperties(){
		Properties prop = new Properties();
		InputStream is = Repository.class.getResourceAsStream("/conf/sys.properties");
		try {
			prop.load(is);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Repository.windowSize=Integer.parseInt(prop.getProperty("windowSize").trim());
		Repository.serverIp=prop.getProperty("serverIp").trim();
		Repository.rmiPort=Integer.parseInt(prop.getProperty("rmiPort").trim()); //22222 default
		Repository.recordInterval=Integer.parseInt(prop.getProperty("recordInterval").trim()); 

		if(prop.getProperty("rmiServiceEnable")==null||prop.getProperty("rmiServiceEnable").equals("false")){
			rmiServiceEnable=false;
		}else{
			rmiServiceEnable=true;
		}
		Repository.exampleURL=prop.getProperty("exampleURL").trim();
		Repository.webServiceNamesStr=prop.getProperty("webServiceNamesStr").trim();
		Repository.webServiceURLsStr=prop.getProperty("webServiceURLsStr").trim();
		Repository.webServiceParamsStr=prop.getProperty("webServiceParamsStr").trim();
		Repository.NUMBER_LC=Integer.parseInt(prop.getProperty("numberOfService").trim()); 
	}	

	public static void initServiceDriver(){
		String[] nameSplits=webServiceNamesStr.split(";");
		String[] urlSplits=webServiceURLsStr.split(";");
		String[] paramSplits=webServiceParamsStr.split(";");
		if(nameSplits.length!=urlSplits.length){
			System.err.println("Repository.initServiceDriver(): nameSplits.length("+nameSplits.length+")!=urlSplits.length("+urlSplits.length+"), system exits");
			System.exit(0);
		}else{
			if(nameSplits.length>=NUMBER_LC){
				System.err.println("Repository.initServiceDriver(): nameSplits.length("+nameSplits.length+")>=NUMBER_LC("+NUMBER_LC+"), system exits");
				System.exit(0);
			}
			for(int i=0;i<nameSplits.length;i++){
				serverDriverList.add(new ThreeTuple<>(nameSplits[i],urlSplits[i],""));
			}
			for(int i=0;i<paramSplits.length;i++){
				serverDriverList.get(i).third=paramSplits[i];
			}
		}
	}
	//init
	private static void initList(){
		for(int i=0;i<NUMBER_LC;i++){
			onlineDataList.add(new ArrayList<Integer>());
			onlineDataListSpec.add(new ArrayList<ThreeTuple<Integer, String, Timestamp>>());
			tempOnlineDataList.add(new ArrayList<Integer>());
			windowOnlineDataList.add(new ArrayList<QueryData>());
			tempWindowOnlineDataList.add(new ArrayList<QueryData>());
		}

		//系统控制变量
		onlineQueryThreadRunning=new boolean[NUMBER_LC]; 
		onlineDataFlag=new boolean[NUMBER_LC]; 
		sendFlag=new boolean[NUMBER_LC]; 

		//负载发生控制变量
		realRequestIntensity=new int[NUMBER_LC]; 
		realQueryIntensity=new int[NUMBER_LC];  
		windowOnLineDataListCount=new int[NUMBER_LC];	
		statisticsCount=new int[NUMBER_LC];	
		totalRequestCount=new int[NUMBER_LC];
		totalQueryCount=new int[NUMBER_LC];

		concurrency=new int[NUMBER_LC];

		latestOnlineData=new QueryData[NUMBER_LC];
		windowAvgPerSec99thQueryTime=new float[NUMBER_LC];
		windowAvgPerSecAvgQueryTime=new float[NUMBER_LC];
	}
	
	
	private static void initLoaderMap(){
		for(int i=0;i<NUMBER_LC;i++){
			loaderMap.put(i,loaderMapping(i)); 
			System.out.println("init loaderMapping: loaderIndex="+i+",loaderDriver="+loaderMap.get(i).getLoaderName()+" url="+loaderMap.get(i).getAbstractJobDriver().queryItemsStr);
		}

	}
	/**
	 * Adds a new data to the window array
	 * Loop assignment in Repository.windowSize
	 * @param data
	 */
	public void addWindowOnlineDataList(QueryData data, int serviceId){
		latestOnlineData[serviceId]=data;
		realQueryIntensity[serviceId]=data.getRealQps();
		synchronized (windowOnlineDataList.get(serviceId)) {
			if(windowOnlineDataList.get(serviceId).size()<windowSize){
				windowOnlineDataList.get(serviceId).add(data);
			}else{
				windowOnlineDataList.get(serviceId).set(windowOnLineDataListCount[serviceId]%windowSize,data);
				windowOnLineDataListCount[serviceId]++;
			}
		}
	}

	/**
	 * Calculate the mean of query time
	 * @return 
	 */
	public float[] getOnlineWindowAvgQueryTime(int serviceId){
		while (windowOnlineDataList.get(serviceId).isEmpty()) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		tempWindowOnlineDataList.get(serviceId).clear();
		synchronized (windowOnlineDataList.get(serviceId)) {
			tempWindowOnlineDataList.get(serviceId).addAll(windowOnlineDataList.get(serviceId));
		}
		int size=tempWindowOnlineDataList.get(serviceId).size();
		float avg99thQueryTime=0;
		float avgAvgQueryTime=0;
		for(QueryData item:tempWindowOnlineDataList.get(serviceId)){
			avg99thQueryTime+=item.getQueryTime99th();
			avgAvgQueryTime+=item.getQueryTimeAvg();
		} 
		avg99thQueryTime=avg99thQueryTime/size; 
		avgAvgQueryTime=avgAvgQueryTime/size;
		windowAvgPerSec99thQueryTime[serviceId]=avg99thQueryTime;
		windowAvgPerSecAvgQueryTime[serviceId]=avgAvgQueryTime;

		return new float[]{avg99thQueryTime,avgAvgQueryTime};
	}
	/**
	 * maps the loaderIndex with the loaderDriver instance
	 * @param loaderIndex
	 * @return
	 */
	private static LoaderDriver loaderMapping(int loaderIndex){
		if(loaderIndex==0){
			return new LoaderDriver("example", ExampleDriver.getInstance());
		}else{
			return new LoaderDriver(serverDriverList.get(loaderIndex-1).first,
					new FuncGraphServiceDriver(serverDriverList.get(loaderIndex-1).second,serverDriverList.get(loaderIndex-1).third));
		}
	} 
}
