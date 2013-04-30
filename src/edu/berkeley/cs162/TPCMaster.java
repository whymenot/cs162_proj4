/**
 * Master for Two-Phase Commits
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
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class TPCMaster {
	
	/**
	 * Implements NetworkHandler to handle registration requests from 
	 * SlaveServers.
	 * 
	 */
	private class TPCRegistrationHandler implements NetworkHandler {

		private ThreadPool threadpool = null;

		public TPCRegistrationHandler() {
			// Call the other constructor
			this(1);	
		}

		public TPCRegistrationHandler(int connections) {
			threadpool = new ThreadPool(connections);	
		}

		@Override
		public void handle(Socket client) throws IOException {
			// implement me
		}
		
		private class RegistrationHandler implements Runnable {
			
			private Socket client = null;

			public RegistrationHandler(Socket client) {
				this.client = client;
			}

			@Override
			public void run() {
				// implement me
			}
		}	
	}
	
	/**
	 *  Data structure to maintain information about SlaveServers
	 *
	 */
	private class SlaveInfo {
		// 64-bit globally unique ID of the SlaveServer
		private long slaveID = -1;
		// Name of the host this SlaveServer is running on
		private String hostName = null;
		// Port which SlaveServer is listening to
		private int port = -1;

		/**
		 * 
		 * @param slaveInfo as "SlaveServerID@HostName:Port"
		 * @throws KVException
		 */
		public SlaveInfo(String slaveInfo) throws KVException {
			// implement me
			try {
				String splitted1[] = slaveInfo.split("@");
				String splitted2[] = splitted1[1].split(":");
				
				this.slaveID = hashTo64bit(splitted1[0]);
				this.hostName = splitted2[0];
				this.port = Integer.parseInt(splitted2[1]);
				
			} catch (Exception e) {
				throw new KVException(new KVMessage("resp", "Registration Error: Received unparseable slave information"));
			}
		}
		
		public long getSlaveID() {
			return slaveID;
		}
		
		public Socket connectHost() throws KVException {
		    // TODO: Optional Implement Me!
			try {
				return new Socket(this.hostName, this.port);
			} catch (Exception e) {
				return null;
			}
		}
		
		public void closeHost(Socket sock) throws KVException {
		    // TODO: Optional Implement Me!
			try {
				sock.close();
			} catch (Exception e) {
				// do nothing
			}
		}
	}
	
	// Timeout value used during 2PC operations
	private static final int TIMEOUT_MILLISECONDS = 5000;
	
	// Cache stored in the Master/Coordinator Server
	private KVCache masterCache = new KVCache(100, 10);
	
	// Registration server that uses TPCRegistrationHandler
	private SocketServer regServer = null;

	// Number of slave servers in the system
	private int numSlaves = -1;
	
	// ID of the next 2PC operation
	private Long tpcOpId = 0L;
	
	// new parameters
	private TreeMap<Long, SlaveInfo> slaveServers;
	
	/**
	 * Creates TPCMaster
	 * 
	 * @param numSlaves number of expected slave servers to register
	 * @throws Exception
	 */
	public TPCMaster(int numSlaves) {
		// Using SlaveInfos from command line just to get the expected number of SlaveServers 
		this.numSlaves = numSlaves;

		// Create registration server
		regServer = new SocketServer("localhost", 9090);
	}
	
	/**
	 * Calculates tpcOpId to be used for an operation. In this implementation
	 * it is a long variable that increases by one for each 2PC operation. 
	 * 
	 * @return 
	 */
	private String getNextTpcOpId() {
		tpcOpId++;
		return tpcOpId.toString();		
	}
	
	/**
	 * Start registration server in a separate thread
	 */
	public void run() {
		AutoGrader.agTPCMasterStarted();
		// implement me
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run(){
				try {
					regServer.connect();
					regServer.run();
				} catch (IOException e) {
					// do nothing
				}
			}
		});
		
		// start the registration thread
		thread.start();
		
		AutoGrader.agTPCMasterFinished();
	}
	
	/**
	 * Converts Strings to 64-bit longs
	 * Borrowed from http://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
	 * Adapted from String.hashCode()
	 * @param string String to hash to 64-bit
	 * @return
	 */
	private long hashTo64bit(String string) {
		// Take a large prime
		long h = 1125899906842597L; 
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31*h + string.charAt(i);
		}
		return h;
	}
	
	/**
	 * Compares two longs as if they were unsigned (Java doesn't have unsigned data types except for char)
	 * Borrowed from http://www.javamex.com/java_equivalents/unsigned_arithmetic.shtml
	 * @param n1 First long
	 * @param n2 Second long
	 * @return is unsigned n1 less than unsigned n2
	 */
	private boolean isLessThanUnsigned(long n1, long n2) {
		return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
	}
	
	private boolean isLessThanEqualUnsigned(long n1, long n2) {
		return isLessThanUnsigned(n1, n2) || n1 == n2;
	}	

	/**
	 * Find first/primary replica location
	 * @param key
	 * @return
	 */
	private SlaveInfo findFirstReplica(String key) {
		// 64-bit hash of the key
		long hashedKey = hashTo64bit(key.toString());

		// implement me
		Entry<Long, SlaveInfo> entry = slaveServers.higherEntry(hashedKey);
		if (entry == null) {
			return slaveServers.firstEntry().getValue();
		} else {
			return entry.getValue();
		}
	}
	
	/**
	 * Find the successor of firstReplica to put the second replica
	 * @param firstReplica
	 * @return
	 */
	private SlaveInfo findSuccessor(SlaveInfo firstReplica) {
		// implement me
		Entry<Long, SlaveInfo> entry = slaveServers.higherEntry(firstReplica.getSlaveID());
		if (entry == null) {
			return slaveServers.firstEntry().getValue();
		} else {
			return entry.getValue();
		}
	}
	
	public KVMessage communicateToSlave(SlaveInfo si, KVMessage msg) throws KVException {
		KVMessage response = null;
		Socket socket = null;
		try {
			socket = si.connectHost();
			msg.sendMessage(socket);
			InputStream is = socket.getInputStream();
			
			response = new KVMessage(is);

		} catch (Exception e) {

		} finally {
			try {
				if (socket != null) socket.close();
			} catch (Exception e) {
				// do nothing
			}
		}
		return response;
	}
	
	/**
	 * Synchronized method to perform 2PC operations one after another
	 * You will need to remove the synchronized declaration if you wish to attempt the extra credit
	 * 
	 * @param msg
	 * @param isPutReq
	 * @throws KVException
	 */
	public synchronized void performTPCOperation(KVMessage msg, boolean isPutReq) throws KVException {
		AutoGrader.agPerformTPCOperationStarted(isPutReq);
		// implement me
		// cache -> masterCache
		String key = msg.getKey();
		
		WriteLock writeLock = masterCache.getWriteLock(key);
		writeLock.lock();
		
		SlaveInfo first, second;
		
		first = this.findFirstReplica(key);
		second = this.findSuccessor(first);
		
		KVMessage request, response;
		
		if (isPutReq) {
			// set put operation
			request = new KVMessage("putreq");
		} else {
			// set del operation
			request = new KVMessage("delreq");
		}
		
		String nextTpcOpId = this.getNextTpcOpId();
		
		request.setKey(key);
		request.setValue(msg.getValue());
		request.setTpcOpId(nextTpcOpId);
		
		try {
			// first phase
			response = this.communicateToSlave(first, request);

			if (response.getMsgType().equals("abort")) {
				while(true) {
					try {
						// second phase
						request = new KVMessage("abort");
						request.setTpcOpId(nextTpcOpId);
						
						response = this.communicateToSlave(first, request);
					
						if (response.getMsgType().equals("ack")) {
							// ack received, exit
							break;
						}
					} catch (KVException e) {
						// retry...
					}
				}
			}
			else {
				response = this.communicateToSlave(second, request);
				
				if (response.getMsgType().equals("abort")) { 
					request = new KVMessage("abort");
				} else {
					request = new KVMessage("commit");
				}
				request.setTpcOpId(nextTpcOpId);
				
				boolean firstReceived = false;
				boolean secondReceived = false;
				while (true) {
					try {
						// second phase
						if (!firstReceived) {
							response = this.communicateToSlave(first, request);
						
							if (response.getMsgType().equals("ack")) {
								// ack received from first slave
								firstReceived = true;
							}
						}
						if (!secondReceived) {
							response = this.communicateToSlave(second, request);
						
							if (response.getMsgType().equals("ack")) {
								// ack received from second slave
								secondReceived = true;
							}
						}
						if (firstReceived && secondReceived) {
							break;
						}
					} catch (KVException e) {
						// retry...
					}
				}
				
				if (response.getMsgType().equals("commit")) {
					if (isPutReq)
						masterCache.put(key, msg.getValue());
					else
						masterCache.del(key);
				}
			}
		} catch (KVException e) {
			throw e;
		} finally {
			writeLock.unlock();
		}
		
		AutoGrader.agPerformTPCOperationFinished(isPutReq);
		return;
	}

	/**
	 * Perform GET operation in the following manner:
	 * - Try to GET from first/primary replica
	 * - If primary succeeded, return Value
	 * - If primary failed, try to GET from the other replica
	 * - If secondary succeeded, return Value
	 * - If secondary failed, return KVExceptions from both replicas
	 * 
	 * @param msg Message containing Key to get
	 * @return Value corresponding to the Key
	 * @throws KVException
	 */
	public String handleGet(KVMessage msg) throws KVException {
		AutoGrader.aghandleGetStarted();
		// implement me
		String toReturn = null;
		String key = msg.getKey();
		
		WriteLock writeLock = masterCache.getWriteLock(key);
		writeLock.lock();
		
		SlaveInfo first, second;
		
		first = this.findFirstReplica(key);
		second = this.findSuccessor(first);
		
		KVMessage request, response;
		
		request = new KVMessage("getreq");
		request.setKey(key);
		
		try {
			if (masterCache.get(key) != null) {
				toReturn = masterCache.get(key);
			} else {
				String error1 = null;
				String error2 = null;
				response = this.communicateToSlave(first, request);
				
				if (response.getMsgType().equals("resp") &&
						response.getKey() != null && response.getKey().equals(key) &&
						response.getValue() != null) {
					// if the first slave has the value
					toReturn = response.getValue();
				}
				else {
					error1 = response.getMessage();
					// try second slave
					response = this.communicateToSlave(second, request);
					
					if (response.getMsgType().equals("resp") &&
							response.getKey() != null && response.getKey().equals(key) &&
							response.getValue() != null) {
						// if the second slave has the value
						toReturn = response.getValue();
					}
					else {
						error2 = response.getMessage();
						// neither of the two slaves has the value
						throw new KVException(new KVMessage("resp", "@" + first.getSlaveID() + ":=" + error1 + "\n@" + second.getSlaveID() + ":=" + error2)); 
					}
				}
			}
		} catch (KVException e) {
			throw e;
		} finally {
			writeLock.unlock();
		}
		
		AutoGrader.aghandleGetFinished();
		return toReturn;
	}
}
