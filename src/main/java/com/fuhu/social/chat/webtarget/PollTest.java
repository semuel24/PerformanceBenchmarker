package com.fuhu.social.chat.webtarget;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.client.Invocation.*;

public class PollTest {

	private static WebTarget target;
	/* semaphore */
	private final static int semaphoreSize = 1000;
	private final static Semaphore semaphore = new Semaphore(semaphoreSize);
	private static ExecutorService threadPoolExecutor;
	private static AtomicLong countR = new AtomicLong(0L);
	private static AtomicLong countERR = new AtomicLong(0L);
	private static Builder bd;

	static {
		// web target
		StringBuilder builder = new StringBuilder();
//		builder.append("http://app01.social-dev.fuhu.org:8881");
		builder.append("http://localhost:8881");
		target = ClientBuilder.newClient().target(
				URI.create(builder.toString()));

		/* executor config */
		int corePoolSize = 1000;
		int maxPoolSize = Integer.MAX_VALUE;
		long keepAliveTime = Long.MAX_VALUE;
		System.out.println("executor pool " + corePoolSize + " -> " + maxPoolSize);
		threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maxPoolSize,
				keepAliveTime, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>());
	}

	public static void main(String[] args) throws InterruptedException {
		
		final long targetcount = Long.parseLong(args[0]);
		final long stime = System.currentTimeMillis(); 
		System.out.println("semaphore size: " + semaphoreSize);
		System.out.println("###started test at time: "
				+ stime + " | to count:" + targetcount);
		
		bd = getbuilder(target, 8647407074254111574L,
				5, "5459a72d-5601-432f-9fd8-528155c09f1b");
		
		for (int i = 0; i < targetcount; i++) {

			semaphore.acquire();
			threadPoolExecutor.submit(new Callable<Object>() {
				public Object call() throws Exception {
					String r = null;
					try{
						r = execPollMessage(bd);
					}catch(Exception e) {
						System.out.println(e);
					}
//					String r = "{\"status\":\"0\"}"; 
					
					semaphore.release();
					if(r == null || !r.contains("\"0\"")) {
						System.out.println(r + " | " + countERR.getAndAdd(1L));
					}
					long _r = countR.addAndGet(1L);
					if (targetcount - _r < 3 ) {//finish loop
						long etime = System.currentTimeMillis(); 
						System.out.println("###fiished test at time: " + etime);
						long lt = (etime-stime)/1000;
						System.out.println("time elapsed: " + lt);
						System.out.println("count/second: " + targetcount/lt);
					}
					return null;
				}
			});

		}
		
		System.out.println("thread waiting " + semaphore.getQueueLength());
		while(semaphore.hasQueuedThreads()) {
			System.out.println("waiting...");
			Thread.sleep(500);
		}
		
		Thread.sleep(5000);//sent requests may not come back yet, so wait for a while
		threadPoolExecutor.shutdown();
		
		

	}
	
	protected static Builder getbuilder(WebTarget target, Long _msgOwnerId,
			int _limit, String _sessKey) {

		return target.path("/chatuser/").path(_msgOwnerId.toString())
				.path("/conversations").queryParam("limit", _limit).request()
				.header("apiKey", UUID.randomUUID().toString())
				.header("sessionKey", _sessKey)
				.header("deviceType", UUID.randomUUID().toString())
				.header("deviceKey", UUID.randomUUID().toString())
				.header("deviceEdition", UUID.randomUUID().toString())
				.header("nabiVersion", UUID.randomUUID().toString());
	}

	protected static String execPollMessage(Builder _bd) {

		return _bd.get(String.class);
	}
}
