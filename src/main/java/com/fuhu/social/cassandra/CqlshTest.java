package com.fuhu.social.cassandra;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;

public class CqlshTest {

public static void main(String [] args) throws InterruptedException {
		
		/*semaphore*/
		final Semaphore semaphore = new Semaphore(3000);
		
		/*canssandra config*/
//		Builder builder = Cluster.builder().withCredentials(null, null);
//		String[] contactPoints = { "localhost" };
		
		//dev cassandra
		Builder builder = Cluster.builder().withCredentials("admin", "Qnmgnps3FtaGo6lBRRyzoMEFGb");
		String[] contactPoints = { "node-a01.csdr-dev.fuhu.org","node-a02.csdr-dev.fuhu.org", "node-b01.csdr-dev.fuhu.org", "node-b02.csdr-dev.fuhu.org"};
		
		//qa cassandra
//		Builder builder = Cluster.builder().withCredentials("admin", "0HL8wcw469HoLnWqL1");
//		String[] contactPoints = { "node-a01.csdr-qa.fuhu.org"};
								    
		for (String cp : contactPoints)
			builder.addContactPoint(cp);
		String keySpace = "social";

		Cluster cluster = builder.build();
		final Session session = cluster.connect(keySpace);
		long beforet = System.currentTimeMillis();
		System.out.println("before test timestamp:" + beforet);
//		final String sesskey = UUID.randomUUID().toString();
		long count = 500000;
		
		
		/*executor config*/
		int  corePoolSize  =   2000;
		int  maxPoolSize   =   3000;
		long keepAliveTime = 500;
		
		ExecutorService threadPoolExecutor =
		        new ThreadPoolExecutor(
		                corePoolSize,
		                maxPoolSize,
		                keepAliveTime,
		                TimeUnit.MILLISECONDS,
		                new LinkedBlockingQueue<Runnable>()
		                );
		
		for(long i=0; i<count; i++) { 
			semaphore.acquire();
			
			threadPoolExecutor.submit(new Callable<Object>() {
				public Object call() throws Exception {
					String sesskey = UUID.randomUUID().toString();
//					session.execute("insert into session (sessionkey, createdate, lastupdatetime, userid) values(" + sesskey + ", 1394690683581, 13946904438876, 8647407074254111575);");
					session.execute("select * from session where sessionkey=" + sesskey + ";");
					semaphore.release();
					return null;
				}
			});
		}
		
		
		while(semaphore.hasQueuedThreads()) {
			Thread.sleep(500);
		}
		
//		threadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		
		
		long aftert = System.currentTimeMillis();
		System.out.println("after test timestamp:" + aftert);
		long elapsedt = aftert-beforet;
		System.out.println("time elapsed: " + elapsedt + " ms");
		elapsedt = elapsedt/1000;
		System.out.println("time elapsed: " + elapsedt + "seconds");
		System.out.println("count/second: " + (count/elapsedt));
		
		cluster.shutdown();
		threadPoolExecutor.shutdown();
	}
}
