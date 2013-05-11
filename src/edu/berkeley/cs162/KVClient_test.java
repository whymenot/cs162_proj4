package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.junit.Test;

public final class KVClient_test {
	@Test
	public void constructor() {
		String server = "tempServer";
		int port = 2222;
		
		KVClient kvc = new KVClient(server,port);
		assertTrue(kvc.getServer().equals(server)
				&& kvc.getPort()==port);
	}
	
	@Test
	public void put() {
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
	
	@Test
	public void get() {
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
					                
					request = new KVMessage("getreq");
					request.setKey("sampleKey");
					request.sendMessage(socket);

					is = socket.getInputStream();
					response = new KVMessage(is);
					assertTrue(response.getMsgType().equals("resp")
							&& response.getKey().equals(request.getKey())
							&& response.getValue().equals("sampleValue"));
					
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
					assertTrue(request.getMsgType().equals("getreq")
							&& request.getKey().equals("sampleKey"));
					
					response = new KVMessage("resp");
					response.setKey(request.getKey());
					response.setValue("sampleValue");
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