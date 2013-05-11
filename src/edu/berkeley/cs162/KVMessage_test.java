package edu.berkeley.cs162;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.Assert.*;
import org.junit.Test;

public class KVMessage_test {
	@Test
	public void constructor1() {
		//Test #1 for constructor1
		try {
			KVMessage kv = new KVMessage("getreq");
			assertTrue(kv.getMsgType().equals("getreq"));
		}
		catch (KVException e) {
			assertTrue(false);
		}
		
		//Test #2 for constructor1
		try {
			KVMessage kv = new KVMessage("wrongType");
			assertTrue(false);
		}
		catch (KVException e) {
			KVMessage kv = e.getMsg();
			assertTrue(kv.getMsgType().equals("resp") && kv.getMessage().equals("Message format incorrect"));
		}
	}
	
	@Test
	public void constructor2() {
		//Test #1 for constructor2
		try {
			KVMessage kv = new KVMessage("resp", "This is a sample message");
			assertTrue(kv.getMsgType().equals("resp") && kv.getMessage().equals("This is a sample message"));
		}
		catch (KVException e) {
			assertTrue(false);
		}
		
		//Test #2 for constructor2
		try {
			KVMessage kv = new KVMessage("wrongType");
			assertTrue(false);
		}
		catch (KVException e) {
			KVMessage kv = e.getMsg();
			assertTrue(kv.getMsgType().equals("resp") && kv.getMessage().equals("Message format incorrect"));
		}
	}
	
	@Test
	public void constructor3() {
		//Test #1 for constructor3
		try {
			KVMessage kvm = new KVMessage("putreq");
			kvm.setKey("sampleKey");
			kvm.setValue("sampleValue");
			String xml = kvm.toXML();
			InputStream stream = new ByteArrayInputStream(xml.getBytes());
			
			KVMessage kvm2 = new KVMessage(stream);
			assertTrue(kvm2.getMsgType().equals("putreq"));
			assertTrue(kvm2.getKey().equals("sampleKey"));
			assertTrue(kvm2.getValue().equals("sampleValue"));
		}
		catch (KVException e) {
			assertTrue(false);
		}
		
		//Test #2 for constructor3
		try {
			KVMessage kvm = new KVMessage("putreq");
			kvm.setKey("sampleKey");
			kvm.setValue("sampleValue");
			kvm.setTpcOpId("sampleTpcOpId");
			String xml = kvm.toXML();
			InputStream stream = new ByteArrayInputStream(xml.getBytes());
			
			KVMessage kvm2 = new KVMessage(stream);
			assertTrue(kvm2.getMsgType().equals("putreq"));
			assertTrue(kvm2.getKey().equals("sampleKey"));
			assertTrue(kvm2.getValue().equals("sampleValue"));
			assertTrue(kvm2.getTpcOpId().equals("sampleTpcOpId"));
		}
		catch (KVException e) {
			assertTrue(false);
		}
		
		//Test #3 for constructor3
		try {
			KVMessage kvm = new KVMessage("abort");
			kvm.setMessage("sampleMessage");
			kvm.setTpcOpId("sampleTpcOpId");
			kvm.setKey("sampleKey");
			String xml = kvm.toXML();
			InputStream stream = new ByteArrayInputStream(xml.getBytes());
			System.out.println(xml);
			
			KVMessage kvm2 = new KVMessage(stream);
			assertTrue(kvm2.getMsgType().equals("abort"));
			assertTrue(kvm2.getMessage().equals("sampleMessage"));
			assertTrue(kvm2.getTpcOpId().equals("sampleTpcOpId"));
		}
		catch (KVException e) {
			assertTrue(false);
		}
	}
	
	@Test
	public void toXML() {
		//Test #1 for toXML
		try {
			KVMessage kv = new KVMessage("getreq");
			kv.setKey("sampleKey");
			String answer = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"getreq\"><Key>sampleKey</Key></KVMessage>";
			assertTrue(kv.toXML().equals(answer));
		}
		catch (KVException e) {
			assertTrue(false);
		}
		
		//Test #2 for toXML
		try {
			KVMessage kv = new KVMessage("putreq");
			kv.setKey("sampleKey");
			kv.setValue("sampleValue");
			String answer = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"putreq\"><Key>sampleKey</Key><Value>sampleValue</Value></KVMessage>";
			assertTrue(kv.toXML().equals(answer));
		}
		catch (KVException e) {
			assertTrue(false);
		}
		
		//Test #3 for toXML
		try {
			KVMessage kv = new KVMessage("delreq");
			kv.setKey("sampleKey");
			String answer = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"delreq\"><Key>sampleKey</Key></KVMessage>";
			assertTrue(kv.toXML().equals(answer));
		}
		catch (KVException e) {
			assertTrue(false);
		}
		
		//Test #4 for toXML
		try {
			KVMessage kv = new KVMessage("resp");
			kv.setMessage("Success");
			String answer = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"resp\"><Message>Success</Message></KVMessage>";
			assertTrue(kv.toXML().equals(answer));
		}
		catch (KVException e) {
			assertTrue(false);
		}
	}
	
	@Test
	public void sendMessage() {
		Thread client = new Thread() {
			public void run() {
				InetAddress server = null;
				int port = 2222;
				Socket socket = null;
				            
				try {
					server = InetAddress.getLocalHost();
					socket = new Socket(server, port);
					                
					KVMessage request = null;
					KVMessage response = null;
					InputStream is = null;
					                
					request = new KVMessage("putreq");
					request.setKey("sampleKey");
					request.setValue("sampleValue");
					request.sendMessage(socket);

					is = socket.getInputStream();
					response = new KVMessage(is);
					assertTrue(response.getMsgType().equals("resp")
							&& response.getMessage().equals("Success"));
					
					socket.close();
				}
				catch (KVException e) {
					System.out.println(e.getMsg().getMessage());
				}
				catch (Exception e) {
					e.printStackTrace();
				}
				finally {
					try {
						socket.close();
					}
					catch (IOException e) {
					            	
					}
				}
			}
		};
		
		Thread server = new Thread() {
			public void run() {
				int port = 2222;
				ServerSocket serverSocket = null;
				Socket socket = null;
				        
				try {
					serverSocket = new ServerSocket(port);
					socket = serverSocket.accept();
								
					KVMessage request = null;
					KVMessage response = null;
					InputStream is = null;
					
					is = socket.getInputStream();
					request = new KVMessage(is);
					assertTrue(request.getMsgType().equals("putreq")
							&& request.getKey().equals("sampleKey")
							&& request.getValue().equals("sampleValue"));
					
					response = new KVMessage("resp","Success");
					response.sendMessage(socket);
					
					socket.close();
					serverSocket.close();
				}
				catch (KVException e) {
					System.out.println(e.getMsg().getMessage());
				}
				catch (Exception e) {
					e.printStackTrace();
				}
				finally {
					try {
						socket.close();
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		server.start();
		client.start();
		
		try {
			server.join();
			client.join();
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}