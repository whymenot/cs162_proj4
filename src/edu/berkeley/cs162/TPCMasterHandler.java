/**
 * Handle TPC connections over a socket interface
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 * 
 * Copyright (c) 2012, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of University of California, Berkeley nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *    
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 *
 */
public class TPCMasterHandler implements NetworkHandler {
	private KVServer kvServer = null;
	private ThreadPool threadpool = null;
	private TPCLog tpcLog = null;
	
	private long slaveID = -1;
	
	// Used to handle the "ignoreNext" message
	private boolean ignoreNext = false;
	
	// States carried from the first to the second phase of a 2PC operation
	private KVMessage originalMessage = null;
	private boolean aborted = true;	

	public TPCMasterHandler(KVServer keyserver) {
		this(keyserver, 1);
	}

	public TPCMasterHandler(KVServer keyserver, long slaveID) {
		this.kvServer = keyserver;
		this.slaveID = slaveID;
		threadpool = new ThreadPool(1);
	}

	public TPCMasterHandler(KVServer kvServer, long slaveID, int connections) {
		this.kvServer = kvServer;
		this.slaveID = slaveID;
		threadpool = new ThreadPool(connections);
	}

	private class ClientHandler implements Runnable {
		private KVServer keyserver = null;
		private Socket client = null;
		
		private void closeConn() {
			try {
				client.close();
			} catch (IOException e) {
			}
		}
		
		@Override
		public void run() {
			// Receive message from client
			// Implement me
			KVMessage msg =null;

			try{
				
				msg = new KVMessage(client.getInputStream());
			}catch(KVException e){System.out.println("exceptionKV");
			}catch(IOException e){System.out.println("exceptionIO");}
			// Parse the message and do stuff 
			String key = msg.getKey();

			
			if (msg.getMsgType().equals("putreq")) {
				handlePut(msg, key);
			}
			else if (msg.getMsgType().equals("getreq")) {
				handleGet(msg, key);
			}
			else if (msg.getMsgType().equals("delreq")) {
				handleDel(msg, key);
			} 
			else if (msg.getMsgType().equals("ignoreNext")) {
				// Set ignoreNext to true. PUT and DEL handlers know what to do.
				// Implement me
				// Send back an acknowledgment
				// Implement me
				ignoreNext = true;
				try {
					KVMessage ackMsg = new KVMessage("ack");
					ackMsg.setTpcOpId(msg.getTpcOpId());
					ackMsg.sendMessage(client);
				} catch(KVException e) {
					//TODO: not sure about this case, maybe should pass along
				}
			}
			else if (msg.getMsgType().equals("commit") || msg.getMsgType().equals("abort")) {
				// Check in TPCLog for the case when SlaveServer is restarted
				// Implement me
				if(tpcLog.hasInterruptedTpcOperation())
					originalMessage = tpcLog.getInterruptedTpcOperation();
				
				handleMasterResponse(msg, originalMessage, aborted);

				
				// Reset state
				// Implement me
				originalMessage = null;
				aborted = true;
			}
			// Finally, close the connection
			closeConn();
		}

		private void handlePut(KVMessage msg, String key) {
			AutoGrader.agTPCPutStarted(slaveID, msg, key);
			System.out.println("hi");
			//write to log and set message
			tpcLog.appendAndFlush(msg); //TODO: check if already written to log
			// Store for use in the second phase
			originalMessage = new KVMessage(msg);
			//check for failure
			if(ignoreNext == true) {
				try{
					KVMessage abortMsg = new KVMessage("abort", "Ignored");
					abortMsg.setTpcOpId( msg.getTpcOpId() );
					tpcLog.appendAndFlush(abortMsg);
					abortMsg.sendMessage( client );//VOTE-ABORT
					ignoreNext = false;
				} catch(KVException e) {
					//TODO: here
				}
			}//Check conditions from Project 3
			else if(key.length() > 256 || originalMessage.getValue().length() > 256*1024) {
				try {
					KVMessage abortMsg = new KVMessage("abort", "Oversized key");
					abortMsg.setTpcOpId( msg.getTpcOpId() );
					tpcLog.appendAndFlush(abortMsg);
					abortMsg.sendMessage(client);//VOTE-ABORT
				} catch(KVException e) {
					//TODO: still need to figure out how to deal with these cases
				}
			} else {
				try {
					aborted = false;
					KVMessage readyMsg = new KVMessage("ready");
					readyMsg.setTpcOpId( msg.getTpcOpId() );
					tpcLog.appendAndFlush(readyMsg);
					readyMsg.sendMessage(client);//VOTE- COMMIT
				} catch(KVException e) {
					//TODO: Same issue
				}
			}
			// Implement me

			AutoGrader.agTPCPutFinished(slaveID, msg, key);
		}
		
 		private void handleGet(KVMessage msg, String key) {
 			AutoGrader.agGetStarted(slaveID);
			
 			// Implement me
 			if (key.length() > 256) {
 				try{
 					KVMessage respMsg = new KVMessage("resp", "Oversized key");
 					respMsg.sendMessage(client);
 				} catch(KVException e) {
 					//TODO: need to figure out how to deal with these exceptions
 				}
 			}
 			else if (!kvServer.hasKey(key)) {
 				try {
	 				KVMessage respMsg = new KVMessage("resp", "Does not exist");
	 				respMsg.sendMessage(client);
 				} catch(KVException e) {
 					//TODO: need to figure out how to deal with these exceptions
 				}
 			}
 			else {
 				try {
	 				KVMessage respMsg = new KVMessage("resp");
	 				respMsg.setKey(key);
	 				respMsg.setValue(kvServer.get(key));
	 				//TODO: Set appropriate key and value for respMsg (Part III);
	 				respMsg.sendMessage(client);
 				} catch(KVException e) {
 					//TODO: need to figure out how to deal with these exceptions
 				}
 			}
 			AutoGrader.agGetFinished(slaveID);
		}
		
		private void handleDel(KVMessage msg, String key) {
			AutoGrader.agTPCDelStarted(slaveID, msg, key);

			tpcLog.appendAndFlush(msg); // should this be original message?
			// Store for use in the second phase
			originalMessage = new KVMessage(msg);
			
			try {
				System.out.println("OriginalMessage : " + originalMessage.toXML());
			} catch (KVException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			// Implement me
			if(ignoreNext == true) {
				try {
					KVMessage abortMsg = new KVMessage("abort", "Ignored");
					abortMsg.setTpcOpId( msg.getTpcOpId() );
					tpcLog.appendAndFlush(abortMsg); 
					abortMsg.sendMessage(client);//VOTE-ABORT
					ignoreNext = false;
				} catch(KVException e) {
 					//TODO: need to figure out how to deal with these exceptions
 				}
			}
			//Check conditions from Project 3
			else if(key.length() > 256) {
				try {
					KVMessage abortMsg = new KVMessage("abort", "Oversized key");
					abortMsg.setTpcOpId( msg.getTpcOpId() );
					tpcLog.appendAndFlush(abortMsg);
					abortMsg.sendMessage(client);//VOTE-ABORT
				} catch(KVException e) {
 					//TODO: need to figure out how to deal with these exceptions
 				}
			}
			else if(!kvServer.hasKey(key)) {
				try {
					KVMessage abortMsg = new KVMessage("abort", "Does not exist");
					abortMsg.setTpcOpId( msg.getTpcOpId() );
					tpcLog.appendAndFlush(abortMsg);
					abortMsg.sendMessage( client );//VOTE-ABORT
				} catch(KVException e) {
 					//TODO: need to figure out how to deal with these exceptions
 				}
			}
			else {
				try {
					aborted = false;
					KVMessage readyMsg = new KVMessage("ready");
					readyMsg.setTpcOpId( msg.getTpcOpId() );
					tpcLog.appendAndFlush(readyMsg);
					readyMsg.sendMessage(client);//VOTE-COMMIT
				} catch(KVException e) {
 					//TODO: need to figure out how to deal with these exceptions
 				}
			}

			
			AutoGrader.agTPCDelFinished(slaveID, msg, key);
		}

		/**
		 * Second phase of 2PC
		 * 
		 * @param masterResp Global decision taken by the master
		 * @param origMsg Message from the actual client (received via the coordinator/master)
		 * @param origAborted Did this slave server abort it in the first phase 
		 */
		private void handleMasterResponse(KVMessage masterResp, KVMessage origMsg, boolean origAborted) {
			AutoGrader.agSecondPhaseStarted(slaveID, origMsg, origAborted);
			//need to not handle same request twice by checking if an ack was sent
			// Implement me
			tpcLog.appendAndFlush(masterResp); 
			if(!origAborted) {
				if(masterResp.getMsgType().equals("commit")) {
					if(origMsg.getMsgType().equals("delreq")) {
						String key = origMsg.getKey();
						try{
							kvServer.del(key);
						}catch(KVException e){
							
						}
					} else if(origMsg.getMsgType().equals("putreq")){
						String key = origMsg.getKey();
						String value = origMsg.getValue();
						
						try{
							kvServer.put(key, value);
						}catch(KVException e){
							
						}
					} else {
						//ignore
					}
				}
			}
			try {
				KVMessage ackMsg = new KVMessage("ack");
				ackMsg.setTpcOpId( masterResp.getTpcOpId() );
				ackMsg.sendMessage( client );
			} catch(KVException e) {
				//TODO: figure out what to do with these exceptions
			}
			
			AutoGrader.agSecondPhaseFinished(slaveID, origMsg, origAborted);
		}

		public ClientHandler(KVServer keyserver, Socket client) {
			this.keyserver = keyserver;
			this.client = client;
		}
	}

	@Override
	public void handle(Socket client) throws IOException {
		AutoGrader.agReceivedTPCRequest(slaveID);
		Runnable r = new ClientHandler(kvServer, client);
		try {
			threadpool.addToQueue(r);
		} catch (InterruptedException e) {
			client.close();
			// TODO: HANDLE ERROR
			return;
		}		
		AutoGrader.agFinishedTPCRequest(slaveID);
	}

	/**
	 * Set TPCLog after it has been rebuilt
	 * @param tpcLog
	 */
	public void setTPCLog(TPCLog tpcLog) {
		this.tpcLog  = tpcLog;
	}

	/**
	 * Registers the slave server with the coordinator
	 * 
	 * @param masterHostName
	 * @param servr KVServer used by this slave server (contains the hostName and a random port)
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws KVException
	 */
	public void registerWithMaster(String masterHostName, SocketServer server) throws UnknownHostException, IOException, KVException {
		AutoGrader.agRegistrationStarted(slaveID);
		
		Socket master = new Socket(masterHostName, 9090);
		KVMessage regMessage = new KVMessage("register", slaveID + "@" + server.getHostname() + ":" + server.getPort());
		regMessage.sendMessage(master);
		
		// Receive master response. 
		// Response should always be success, except for Exceptions. Throw away.
		new KVMessage(master.getInputStream());
		
		master.close();
		AutoGrader.agRegistrationFinished(slaveID);
	}
}
