package edu.berkeley.cs162;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.Assert.*;
import org.junit.Test;

public class Integration_test {
	@Test
	public void put() {
		Thread client = new Thread() {
			public void run() {
				InetAddress server = null;
				int port = 8080;
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
		
		Thread TPCserver = new Thread() {
			public void run() {
		
				SocketServer server = null;
				TPCMaster tpcMaster = null;
				
				try {
					tpcMaster = new TPCMaster(2);
					tpcMaster.run();
					
					// Create KVClientHandler
					System.out.println("Binding Master(TPCMaster):");
					server = new SocketServer(InetAddress.getLocalHost().getHostAddress(), 8080);
					NetworkHandler handler = new KVClientHandler(tpcMaster);
					server.addHandler(handler);
					server.connect();
					System.out.println("Starting Master(TPCMaster)");
					server.run();
		
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		
		Thread slave1 = new Thread() {
			public void run() {
				String logPath = null;
				TPCLog tpcLog = null;	
				
				KVServer keyServer = null;
				SocketServer slaveServer = null;
				
				long slaveID = -1;
				String masterHostName = null;
		
				try {
					slaveID = 10000;
					masterHostName = InetAddress.getLocalHost().getHostName();
					
					// Create TPCMasterHandler
					System.out.println("Binding SlaveServer1:");
					keyServer = new KVServer(100, 10);
					slaveServer = new SocketServer(InetAddress.getLocalHost().getHostAddress());
					TPCMasterHandler handler = new TPCMasterHandler(keyServer, slaveID);
					slaveServer.addHandler(handler);
					slaveServer.connect();
					
					// Create TPCLog
					logPath = slaveID + "@" + slaveServer.getHostname();
					tpcLog = new TPCLog(logPath, keyServer);
					
					// Load from disk and rebuild logs
					tpcLog.rebuildKeyServer();
					
					// Set log for TPCMasterHandler
					handler.setTPCLog(tpcLog);
					
					// Register with the Master. Assuming it always succeeds (not catching).
					handler.registerWithMaster(masterHostName, slaveServer);
					
					System.out.println("Starting SlaveServer at " + slaveServer.getHostname() + ":" + slaveServer.getPort());
					slaveServer.run();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		
		Thread slave2 = new Thread() {
			public void run() {
				String logPath = null;
				TPCLog tpcLog = null;	
				
				KVServer keyServer = null;
				SocketServer slaveServer = null;
				
				long slaveID = -1;
				String masterHostName = null;
				try {
					slaveID = 20000;
					masterHostName = InetAddress.getLocalHost().getHostName();
					
					// Create TPCMasterHandler
					System.out.println("Binding SlaveServer2:");
					keyServer = new KVServer(100, 10);
					slaveServer = new SocketServer(InetAddress.getLocalHost().getHostAddress());
					TPCMasterHandler handler = new TPCMasterHandler(keyServer, slaveID);
					slaveServer.addHandler(handler);
					slaveServer.connect();
					
					// Create TPCLog
					logPath = slaveID + "@" + slaveServer.getHostname();
					tpcLog = new TPCLog(logPath, keyServer);
					
					// Load from disk and rebuild logs
					tpcLog.rebuildKeyServer();
					
					// Set log for TPCMasterHandler
					handler.setTPCLog(tpcLog);
					
					// Register with the Master. Assuming it always succeeds (not catching).
					handler.registerWithMaster(masterHostName, slaveServer);
					
					System.out.println("Starting SlaveServer at " + slaveServer.getHostname() + ":" + slaveServer.getPort());
					slaveServer.run();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};

		TPCserver.start(); // coordination server
		slave1.start();
		slave2.start();
		client.start();

		try {
			// i have 1 client,
			// 1 TPCMaster
			// 2 Slaves.
			TPCserver.join();
			slave1.join();
			slave2.join();
			client.join();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
/*
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
	*/
}