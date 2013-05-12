package edu.berkeley.cs162;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.junit.Test;

import java.net.ServerSocket;

public class TPCMasterHandler_test {
	private class ServerThread extends Thread {
		private boolean b=false;
		public Socket listenClient = null;
		public void run(){
			ServerSocket serverSocket = null;
			try{
			serverSocket = new ServerSocket(9090);
			this.listenClient = serverSocket.accept();
			System.out.println("listen client not null");
			}catch(IOException E){}
			
			//catch(InterruptedException e){}
			
		}
		public boolean isSetUp(){
			return this.b;
		}
	}
	@Test
	public void test() throws Exception{
		
		
		ServerThread server = new ServerThread();
			
		/*Thread client = new Thread() {
			public void run(){
				Socket client = null;
				try{
					//this.wait(10000);
					System.out.println("send setting up");
					client = new Socket(InetAddress.getLocalHost(),9090);
					System.out.println("send set up");
				}catch(UnknownHostException e){}catch(IOException e){}//catch(InterruptedException e){}
				KVMessage kvm =null;
				try{
				kvm = new KVMessage("putreq");
				}catch(KVException e){}
				kvm.setKey("sampleKey");
				kvm.setValue("sampleValue");
				kvm.setTpcOpId("sampleTpcOpId");
				try{
					
					kvm.sendMessage(client);
				}catch(KVException e){}
			}
		};*/
		
		server.start();
		Thread.yield();
		
		Socket client = null;
				try{
					
				client = new Socket(InetAddress.getLocalHost(),9090);
				System.out.println("send set up");
				}catch(UnknownHostException e){}catch(IOException e){}//catch(InterruptedException e){}
				
				
				
				KVServer keyServer = new KVServer(100, 10);
				TPCMasterHandler handler = new TPCMasterHandler(keyServer,9090);
				try{
				  handler.setTPCLog(new TPCLog("",keyServer));
				  //while(server.listenClient==null){}
				  handler.handle(server.listenClient);
				  System.out.println("reception set up");
				}catch(IOException E){}
				
				
			
				KVMessage kvm =null;
				try{
				kvm = new KVMessage("putreq");
				}catch(KVException e){}
				kvm.setKey("sampleKey");
				kvm.setValue("sampleValue");
				kvm.setTpcOpId("sampleTpcOpId");
				
				System.out.println("sending message");
				try{
					kvm.sendMessage(client);
					System.out.println("sent message");
				}catch(KVException e){System.out.println("no sent message");}
				
		
		
		try {
			server.join();
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
