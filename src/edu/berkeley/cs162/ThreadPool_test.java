package edu.berkeley.cs162;

import static org.junit.Assert.*;
import org.junit.Test;

public class ThreadPool_test {
	
	private static int numFinishedJob = 0;
	
	
	private static Runnable createJob (final int jobID) {
		return new Runnable() {
			public void run() {
				System.out.println("Job started - " + jobID);
				try {
					Thread.sleep(500);
					numFinishedJob++;
				} catch (InterruptedException ex) {
					// exception!
				}
				System.out.println("Job ended - " + jobID);
			}
		};
	}
	
	@Test
	public void ThreadPoolTest1() {
		// testing 50 of the size of ThreadPool and Jobs.
		
		int numThreadPool = 50;
		int numThreads = 50;
		
		numFinishedJob = 0;
		
		ThreadPool threadPool = new ThreadPool(numThreadPool);
		
		for(int i = 0; i < numThreads; i++) {
			try { 
				threadPool.addToQueue(createJob(i));
			}
			catch (InterruptedException ex) {
				
			}
		}
		
		// wait until jobs are done
		try {
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
			// exception
		}
		
		assertEquals(numThreads, numFinishedJob);
	}
	
	@Test
	public void ThreadPoolTest2() {
		// testing 10 of the size of ThreadPool, and 20 Jobs.
		
		int numThreadPool = 10;
		int numThreads = 20;
		
		numFinishedJob = 0;
		
		ThreadPool threadPool = new ThreadPool(numThreadPool);
		
		for(int i = 0; i < numThreads; i++) {
			try { 
				threadPool.addToQueue(createJob(i));
			}
			catch (InterruptedException ex) {
				
			}
		}
		
		// wait until jobs are done
		try {
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
			// exception
		}
		
		assertEquals(numThreads, numFinishedJob);
	}	
}