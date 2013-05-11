/**
 * Log for Two-Phase Commit
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
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
 *  DISCLAIMED. IN NO EVENT SHALL PRASHANTH MOHAN BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class TPCLog {
    //Fields
	private String logPath = null; // Path to log file
	private KVServer kvServer = null; // Reference to the KVServer of this slave. Populated by rebuildKeyServer()
	private ArrayList<KVMessage> entries = null; // Log entries
	private KVMessage interruptedTpcOperation = null;
	/*  Keeps track of the interrupted 2PC operation.
	 There can be at most one, i.e., when the last 2PC operation before
	 crashing was in READY state.
	 Set in  rebuildKeyServer() during recovery */ 
	
    //Constructor
	/**
	 * @param logPath 
	 * @param kvServer Reference to the KVServer of this slave. Populated by
	 * rebuildKeyServer() during start. 
	 */
	public TPCLog(String logPath, KVServer kvServer) {
		this.logPath = logPath;
		entries = null;
		this.kvServer = kvServer;
	}

    //Helper Methods
	public ArrayList<KVMessage> getEntries() {
		return entries;
	}

	public boolean empty() {
		return (entries.size() == 0);
	}

    public void setInterruptedTpcOperation(KVMessage kvm) {
        String type = kvm.getMsgType();
        if (type.equals("putreq") || type.equals("delreq"))
            interruptedTpcOperation = kvm;
    }
	
    //Action Methods
	public void appendAndFlush(KVMessage entry) {
        String msgType = entry.getMsgType();
        if (msgType.equals("getreq") || msgType.equals("putreq") ||
            msgType.equals("abort") || msgType.equals("commit")) {
            loadFromDisk();
            entries.add(entry);
            flushToDisk();
	    }
    }

	/**
	 * Load log from persistent storage
	 */
	@SuppressWarnings("unchecked")
	public void loadFromDisk() {
		ObjectInputStream inputStream = null;
		
		try {
			inputStream = new ObjectInputStream(new FileInputStream(logPath));			
			entries = (ArrayList<KVMessage>) inputStream.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// If log never existed, there are no entries
			if (entries == null) {
				entries = new ArrayList<KVMessage>();
			}

			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Writes log to persistent storage
	 */
	public void flushToDisk() {
		ObjectOutputStream outputStream = null;
		
		try {
			outputStream = new ObjectOutputStream(new FileOutputStream(logPath));
			outputStream.writeObject(entries);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (outputStream != null) {
					outputStream.flush();
					outputStream.close();
				}
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Load log and rebuild by iterating over log entries
	 * Set interruptedTpcOperation, if there is one (i.e., SlaveServer crashed
	 * in the READY state)
	 * @throws KVException
	 */
	public void rebuildKeyServer() throws KVException {
        KVMessage request = null;
        KVMessage response = null;
        String reqType = null;
        String respType = null;
        String reqId = null;
        String respId = null;
        int i = 0;
        int increment = 1;

        loadFromDisk();
        if (entries.size() == 1)
            setInterruptedTpcOperation(entries.get(i));
        while (i+1 < entries.size()) { //entries.size()-1 >= i+1
            increment = 1;
            request = entries.get(i);
            response = entries.get(i+1);
            reqType = request.getMsgType();
            respType = response.getMsgType();
            reqId = request.getTpcOpId();
            respId = response.getTpcOpId();

            if (respType.equals("commit")) {
                if (reqType.equals("putreq") && reqId.equals(respId))
                    kvServer.put(request.getKey(), request.getValue());
                if (reqType.equals("delreq") && reqId.equals(respId))
                    kvServer.del(request.getKey());
                increment = 2;
            }
            i += increment;
            
            if (i == entries.size()-1)
                setInterruptedTpcOperation(entries.get(i));
        }
	}
	
	/**
	 * @return Interrupted 2PC operation, if any 
	 */
	public KVMessage getInterruptedTpcOperation() { 
		KVMessage logEntry = interruptedTpcOperation; 
		interruptedTpcOperation = null; 
		return logEntry; 
	}
	
	/**
	 * @return True if TPCLog contains an interrupted 2PC operation
	 */
	public boolean hasInterruptedTpcOperation() {
		return interruptedTpcOperation != null;
	}
}
