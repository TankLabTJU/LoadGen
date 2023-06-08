package scs.util.loadGen.driver.serverless;
  
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import scs.util.loadGen.driver.AbstractJobDriver;
import scs.util.loadGen.threads.LoadExecThread;
import scs.util.repository.Repository;
import scs.util.tools.HttpClientPool; 
/**
 * Image recognition service request class
 * GPU inference
 * @author Yanan Yang
 *
 */
public class FuncGraphServiceDriver extends AbstractJobDriver{
	/**
	 * Singleton code block
	 */
	
	public FuncGraphServiceDriver(String queryItemStr,String jsonParamStr) {
		// TODO Auto-generated constructor stub
		this.queryItemsStr=queryItemStr;
		this.jsonParmStr=jsonParamStr;
		initVariables();
	}
 
	@Override
	protected void initVariables() {
		httpClient=HttpClientPool.getInstance().getConnection();
	}

	/**
	 * using countDown to send requests in open-loop
	 */
	public void executeJob(int serviceId) {
		ExecutorService executor = Executors.newCachedThreadPool();
	 
		Repository.onlineQueryThreadRunning[serviceId]=true;
		Repository.sendFlag[serviceId]=true;
		while(Repository.onlineDataFlag[serviceId]==true){
			if(Repository.sendFlag[serviceId]==true){
				CountDownLatch begin=new CountDownLatch(1);
				if (Repository.realRequestIntensity[serviceId]==0){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					int sleepUnit=1000/Repository.realRequestIntensity[serviceId];
					for (int i=0;i<Repository.realRequestIntensity[serviceId];i++){ 
						executor.execute(new LoadExecThread(httpClient,queryItemsStr,begin,serviceId,jsonParmStr+random.nextInt(Integer.MAX_VALUE),sleepUnit*i,"GET"));
					}
				}
				Repository.sendFlag[serviceId]=false;
				Repository.totalRequestCount[serviceId]+=Repository.realRequestIntensity[serviceId];
				begin.countDown();
			}else{
				try {
					Thread.sleep(10);
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