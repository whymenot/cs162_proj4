package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Dictionary;

import org.junit.Test;

public class TPCLog_test {
	@Test
	public void test() {
		System.out.println("Test #1");
		Thread writer = new Thread() {
			public void run() {
				KVServer server = new KVServer(1,10);
				TPCLog log = new TPCLog("temp1", server);
				
				try {
					KVMessage k1 = new KVMessage("getreq");
					k1.setKey("k1_key");
					KVMessage k2 = new KVMessage("putreq");
					k2.setKey("k2_key");
					k2.setValue("k2_val");
					KVMessage k3 = new KVMessage("delreq");
					k3.setKey("k3_key");
					
					assertTrue(log.getEntries() == null);
					log.appendAndFlush(k1);
					assertTrue(log.getEntries() == null);
					log.appendAndFlush(k2);
					assertTrue(log.getEntries().size() == 1);
					log.appendAndFlush(k3);
					assertTrue(log.getEntries().size() == 2);
				} catch (Exception e) { e.printStackTrace(); }
			}
		};
		
		Thread reader = new Thread() {
			public void run() {
				KVServer server = new KVServer(1,10);
				TPCLog log = new TPCLog("temp1", server);
				
				try {
					KVMessage k1 = new KVMessage("getreq");
					k1.setKey("k1_key");
					KVMessage k2 = new KVMessage("putreq");
					k2.setKey("k2_key");
					k2.setValue("k2_val");
					KVMessage k3 = new KVMessage("delreq");
					k3.setKey("k3_key");
					
					log.loadFromDisk();
					for (int i=0; i<log.getEntries().size(); i++) {
						String result = log.getEntries().get(i).toXML();
						System.out.println(result);
						
						if (i==0) assertTrue(result.equals(k2.toXML()));
						if (i==1) assertTrue(result.equals(k3.toXML()));
					}
				}
				catch (Exception e) { e.printStackTrace(); }
				finally {
					File file = new File("temp1");
					file.delete();
				}
			}
		};
		
		writer.start();
		try {
			writer.join();
			System.out.println("Writer Done");
		} catch (Exception e) { e.printStackTrace(); }
		
		reader.start();
		try {
			reader.join();
			System.out.println("Reader Done");
		} catch (Exception e) { e.printStackTrace(); }
		
		System.out.println("");
	}
	
	
	@Test
	public void test2() {
		System.out.println("Test #2");
		Thread writer = new Thread() {
			public void run() {
				KVServer server = new KVServer(1,10);
				TPCLog log = new TPCLog("temp2", server);
				
				try {
					KVMessage commit = new KVMessage("commit");
					commit.setTpcOpId("1234");
					KVMessage abort = new KVMessage("abort");
					abort.setTpcOpId("1234");
					
					KVMessage k1 = new KVMessage("putreq");
					k1.setKey("k1_key");
					k1.setValue("k1_val");
					k1.setTpcOpId("1234");
					KVMessage k2 = new KVMessage("putreq");
					k2.setKey("k2_key");
					k2.setValue("k2_val");
					k2.setTpcOpId("1234");
					KVMessage k3 = new KVMessage("putreq");
					k3.setKey("k3_key");
					k3.setValue("k3_val");
					k3.setTpcOpId("1234");
					KVMessage k4 = new KVMessage("delreq");
					k4.setKey("k3_key");
					k4.setTpcOpId("1234");
					
					
					log.appendAndFlush(k1);
					log.appendAndFlush(commit);
					log.appendAndFlush(k2);
					log.appendAndFlush(commit);
					log.appendAndFlush(k3);
					log.appendAndFlush(commit);
					log.appendAndFlush(k4);
					log.appendAndFlush(commit);
				} catch (Exception e) { e.printStackTrace(); }
			}
		};
		
		Thread reader = new Thread() {
			public void run() {
				KVServer server = new KVServer(1,10);
				TPCLog log = new TPCLog("temp2", server);
				KVStore kvs;
				Dictionary<String, String> dict;
				
				try {
					log.loadFromDisk();
					log.rebuildKeyServer();
					kvs = server.getKVStore();
					dict = kvs.getStore();
					
					assertTrue(dict.size() == 2);
					assertTrue(dict.get("k1_key").equals("k1_val"));
					System.out.println("Key = k1_key, Value = " + dict.get("k1_key"));
					assertTrue(dict.get("k2_key").equals("k2_val"));
					System.out.println("Key = k2_key, Value = " + dict.get("k2_key"));
					
					System.out.print("Key = k3_key, Value = ");
					System.out.println(dict.get("k3_key"));
				}
				catch (Exception e) { e.printStackTrace(); }
				finally {
					File file = new File("temp2");
					file.delete();
				}
			}
		};
		
		writer.start();
		try {
			writer.join();
			System.out.println("Writer Done");
		} catch (Exception e) { e.printStackTrace(); }
		
		reader.start();
		try {
			reader.join();
			System.out.println("Reader Done");
		} catch (Exception e) { e.printStackTrace(); }
		
		System.out.println("");
	}
	
	
	@Test
	public void test3() {
		System.out.println("Test #3");
		Thread writer = new Thread() {
			public void run() {
				KVServer server = new KVServer(1,10);
				TPCLog log = new TPCLog("temp3", server);
				
				try {
					KVMessage commit = new KVMessage("commit");
					commit.setTpcOpId("1234");
					KVMessage abort = new KVMessage("abort");
					abort.setTpcOpId("1234");
					
					KVMessage k1 = new KVMessage("putreq");
					k1.setKey("k1_key");
					k1.setValue("k1_val");
					k1.setTpcOpId("1234");
					KVMessage k2 = new KVMessage("putreq");
					k2.setKey("k2_key");
					k2.setValue("k2_val");
					k2.setTpcOpId("1234");
					KVMessage k3 = new KVMessage("putreq");
					k3.setKey("k3_key");
					k3.setValue("k3_val");
					k3.setTpcOpId("1234");
					
					
					log.appendAndFlush(k1);
					log.appendAndFlush(abort);
					log.appendAndFlush(abort);
					log.appendAndFlush(k2);
					log.appendAndFlush(commit);
					log.appendAndFlush(k3);
				} catch (Exception e) { e.printStackTrace(); }
			}
		};
		
		Thread reader = new Thread() {
			public void run() {
				KVServer server = new KVServer(1,10);
				TPCLog log = new TPCLog("temp3", server);
				KVStore kvs;
				Dictionary<String, String> dict;
				
				try {
					log.loadFromDisk();
					log.rebuildKeyServer();
					kvs = server.getKVStore();
					dict = kvs.getStore();
					
					assertTrue(dict.size() == 1);
					System.out.print("Key = k1_key, Value = ");
					System.out.println(dict.get("k1_key"));
					
					assertTrue(dict.get("k2_key").equals("k2_val"));
					System.out.println("Key = k2_key, Value = " + dict.get("k2_key"));
					
					System.out.print("Key = k3_key, Value = ");
					System.out.println(dict.get("k3_key"));
				}
				catch (Exception e) { e.printStackTrace(); }
				finally {
					File file = new File("temp3");
					file.delete();
				}
			}
		};
		
		writer.start();
		try {
			writer.join();
			System.out.println("Writer Done");
		} catch (Exception e) { e.printStackTrace(); }
		
		reader.start();
		try {
			reader.join();
			System.out.println("Reader Done");
		} catch (Exception e) { e.printStackTrace(); }
		
		System.out.println("");
	}
}
