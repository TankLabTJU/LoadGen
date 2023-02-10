package scs.util.loadGen.driver.sdcbench;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import scs.util.loadGen.driver.AbstractJobDriver;
import scs.util.loadGen.threads.LoadExecThread;
import scs.util.repository.Repository;
import scs.util.tools.HttpClientPool;
import scs.util.tools.RandomString; 
/**
 * Image recognition service request class
 * GPU inference
 * @author Yanan Yang
 *
 */
public class SdcbenchSolrDriver extends AbstractJobDriver{
	/**
	 * Singleton code block
	 */
	private static SdcbenchSolrDriver driver=null;	


	public SdcbenchSolrDriver(){initVariables();}
	public synchronized static SdcbenchSolrDriver getInstance() {
		if (driver == null) {
			driver = new SdcbenchSolrDriver();
		}
		return driver;
	}

	@Override
	protected void initVariables() {
		
		httpClient=HttpClientPool.getInstance().getConnection();
		queryItemsStr=Repository.solrSearchBaseURL;
		//jsonParmStr=Repository.solrSearchParmStr;
		jsonParmStr="q=id:[x%20TO%20*]&&y~0.8&rows=100&start=0";
		
		queryItemsStr=queryItemsStr.replace("Ip", "192.168.1.106"); 
		queryItemsStr=queryItemsStr.replace("Port", "30002"); 
	}

	/**
	 * using countDown to send requests in open-loop
	 */
	public void executeJob(int serviceId) {
		ExecutorService executor = Executors.newCachedThreadPool();
		Random rand=new Random();
		Repository.onlineQueryThreadRunning[serviceId]=true;
		Repository.sendFlag[serviceId]=true;
		while(Repository.onlineDataFlag[serviceId]==true){
			if(Repository.sendFlag[serviceId]==true&&Repository.realRequestIntensity[serviceId]>0){
				CountDownLatch begin=new CountDownLatch(1);
				int sleepUnit=1000/Repository.realRequestIntensity[serviceId];
				for (int i=0;i<Repository.realRequestIntensity[serviceId];i++){
					jsonParmStr=jsonParmStr.replace("x", Integer.toString(rand.nextInt(100000)));
					jsonParmStr=jsonParmStr.replace("y", RandomString.generateString(1));
					executor.execute(new LoadExecThread(httpClient,queryItemsStr+jsonParmStr,begin,serviceId,"",sleepUnit*i,"GET"));
				}
				Repository.sendFlag[serviceId]=false;
				Repository.totalRequestCount[serviceId]+=Repository.realRequestIntensity[serviceId];
				begin.countDown();
			}else{
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				//System.out.println("loader watting "+TestRepository.list.size());
			}
		}
		executor.shutdown();
		while(!executor.isTerminated()){
			try {
				Thread.sleep(2000);
			} catch(InterruptedException e){
				e.printStackTrace();
			}
		}  
		Repository.onlineQueryThreadRunning[serviceId]=false; 
	}




}