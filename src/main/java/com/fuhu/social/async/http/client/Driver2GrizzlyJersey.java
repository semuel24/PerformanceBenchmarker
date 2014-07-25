package com.fuhu.social.async.http.client;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

public class Driver2GrizzlyJersey {

	private static AtomicInteger errC1 = new AtomicInteger(0);
	private static AtomicInteger errC2 = new AtomicInteger(0);
	private static Semaphore semaphore = null;
	public static void main(String [] args) throws IOException, InterruptedException, ExecutionException {
		
		int semaphoreSize = Integer.valueOf(args[0]);
		int total = Integer.valueOf(args[1]);
		String host = args[2];
		
		semaphore = new Semaphore(semaphoreSize);
		long stime = System.currentTimeMillis(); 
		System.out.println("semaphore size: " + semaphoreSize);
		System.out.println("###started test at time: " + stime + " | to count:" + total);
		 
		AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
		
		//start to send request
		for(int i=0; i<total; i++) {
			semaphore.acquire();
			request(asyncHttpClient, host);
		}
		
		System.out.println("thread waiting " + semaphore.getQueueLength());
		//guarantee all requests are handled beyond this line
		while(semaphore.hasQueuedThreads() || semaphore.availablePermits() != semaphoreSize) {
			Thread.sleep(100);
		}
		
		long etime = System.currentTimeMillis(); 
		System.out.println("###fiished test at time: " + etime);
		long lt = (etime-stime)/1000;
		System.out.println("time elapsed: " + lt);
		System.out.println("count/second: " + total/lt);
		
		if (!asyncHttpClient.isClosed()) {
			asyncHttpClient.close();
		}
	}
	
	//host = :8881/chatuser/1/conversations  sync
	//host = :8881/conversation/1/messages   async
	private static void request(AsyncHttpClient asyncHttpClient, String _host) throws IOException {
		
		asyncHttpClient
				.prepareGet(
						"http://" + _host)
				.addHeader("Accept", "application/json")
				.addHeader("Content-Type", "application/json")
				.addHeader("sessionKey", "5459a72d-5601-432f-9fd8-528155c09f1b")
				.execute(new AsyncCompletionHandler<Integer>() {

					@Override
					public Integer onCompleted(Response response)
							throws Exception {

						String r = response.getResponseBody();
						if (r == null || !r.contains("\"0\"")) {
							System.out.println("error response: " + r
									+ " | and error count:"
									+ errC1.addAndGet(0));
						}
						semaphore.release();
						return response.getStatusCode();
					}

					@Override
					public void onThrowable(Throwable t) {
						semaphore.release();
						System.out.println(t.toString());
						System.out.println("throwable count: " + errC2.addAndGet(1));
					}
				});

		
	}
}
