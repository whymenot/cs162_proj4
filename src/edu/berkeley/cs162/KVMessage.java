/**
 * XML Parsing library for the key-value store
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

import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.net.Socket;

/** Part I */
import edu.berkeley.cs162.KVException;

import java.io.OutputStream;
import java.io.StringWriter;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;

import org.xml.sax.SAXException;
/** Part I END */


/**
 * This is the object that is used to generate messages the XML based messages 
 * for communication between clients and servers. 
 */
public class KVMessage implements Serializable {
    //Fields
	private static final long serialVersionUID = 6473128480951955693L;
	private String msgType = null;
	private String key = null;
	private String value = null;
	private String message = null;
    private String tpcOpId = null;    
	
	//Helper Methods
	public final String getMsgType() { return msgType; }
	public final String getKey() { return key; }
	public final String getValue() { return value; }
	public final String getStatus() { return status; }
	public final String getMessage() { return message; }
	public final String getTpcOpId() { return tpcOpId; }

    public final void setMsgType(String msgType) { this.msgType = msgType; }
	public final void setKey(String key) { this.key = key; }
	public final void setValue(String value) { this.value = value; }
	public final void setStatus(String status) { this.status = status; }
	public final void setMessage(String message) { this.message = message; }
	public final void setTpcOpId(String tpcOpId) { this.tpcOpId = tpcOpId; }

	/** Part I */
	private boolean validMsgType(String msgType) {
		if (msgType.equals("getreq") ||
            msgType.equals("putreq") ||
            msgType.equals("delreq") ||
            msgType.equals("resp") ||
            msgType.equals("ready") ||
            msgType.equals("abort") ||
            msgType.equals("commit/abort") ||
            msgType.equals("ack") ||
            msgType.equals("register") ||
            msgType.equals("ignoreNext"))
			return true;
		else
			return false;
	}

	//Constructors
	/***
	 * 
	 * @param msgType
	 * @throws KVException of type "resp" with message "Message format incorrect" if msgType is unknown
	 */
	public KVMessage(String msgType) throws KVException {
		if (validMsgType(msgType) == false)
			throw new KVException(new KVMessage("resp", "Message format incorrect"));
		else
			this.msgType = msgType;
	}
	
	public KVMessage(String msgType, String message) throws KVException {
		if (validMsgType(msgType) == false)
			throw new KVException(new KVMessage("resp", "Message format incorrect"));
		else if (message == null || message.length()==0)
			throw new KVException(new KVMessage("resp", "Message format incorrect"));
		else {
			this.msgType = msgType;
			this.message = message;
		}
	}
	
	 /***
     * Parse KVMessage from incoming network connection
     * @param sock
     * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
     * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
     * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
     * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
     */
	public KVMessage(InputStream input) throws KVException {
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(new NoCloseInputStream(input));

			Element root = doc.getDocumentElement();
			root.normalize();
			
			if (!root.getNodeName().equals("KVMessage"))
				throw new KVException(new KVMessage("resp", "Message format incorrect"));

			this.msgType = root.getAttribute("type");
			NodeList keyList = root.getElementsByTagName("Key");
			NodeList valueList = root.getElementsByTagName("Value");
			NodeList messageList = root.getElementsByTagName("Message");
            NodeList tpcOpIdList = root.getElementsByTagName("TPCOpId");

			if (keyList.getLength()>1 || valueList.getLength()>1 || messageList.getLength()>1 || tpcOpIdList.getLength()>1)
				throw new KVException(new KVMessage("resp", "Message format incorrect"));
			
			if (this.msgType.equals("getreq")) {
				if (keyList.getLength()==0)
					throw new KVException(new KVMessage("resp", "Message format incorrect"));
                
                String key = keyList.item(0).getTextContent();
                if (key==null || key.length()==0)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                this.key = key;
                // TODO
			}
			else if (this.msgType.equals("putreq")) {
				if (keyList.getLength()==0 || valueList.getLength()==0 || tpcOpIdList.getLength()==0)
					throw new KVException(new KVMessage("resp", "Message format incorrect"));
                
                String key = keyList.item(0).getTextContent();
                String value = valueList.item(0).getTextContent();
                String tpcOpId = tpcOpIdList.item(0).getTextContent();
                if (key==null || key.length()==0 || value==null || value.length()==0 || tpcOpId==null || tpcOpId.length()==0)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                this.key = key;
                this.value = value;
                this.tpcOpId = tpcOpId;
			}
			else if (this.msgType.equals("delreq")) {
				if (keyList.getLength()==0 || tpcOpIdList.getLength()==0)
					throw new KVException(new KVMessage("resp", "Message format incorrect"));
                
                String key = keyList.item(0).getTextContent();
                String tpcOpId = tpcOpIdList.item(0).getTextContent();
                if (key==null || key.length()==0 || tpcOpId==null || tpcOpId.length()==0)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                this.key = key;
                this.tpcOpId = tpcOpId;
			}
			else if (this.msgType.equals("resp")) {
				if (messageList.getLength() == 0) {
					if (keyList.getLength() == 0 || valueList.getLength() == 0)
						throw new KVException(new KVMessage("resp", "Message format incorrect"));
						
                    String key = keyList.item(0).getTextContent();
                    String value = valueList.item(0).getTextContent();
                    if (key==null || key.length()==0 || value==null || value.length()==0)
                        throw new KVException(new KVMessage("resp", "Message format incorrect"));
                    this.key = key;
                    this.value = value;
				}
				else {
					String message = messageList.item(0).getTextContent();
					if (message==null || message.length()==0)
						throw new KVException(new KVMessage("resp", "Message format incorrect"));
					this.message = message;
				}
			}
            else if (this.msgType.equals("ready")) {
				if (tpcOpIdList.getLength()==0)
					throw new KVException(new KVMessage("resp", "Message format incorrect"));
                
                String tpcOpId = tpcOpIdList.item(0).getTextContent();
                if (tpcOpId==null || tpcOpId.length()==0)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                this.tpcOpId = tpcOpId;
            }
            else if (this.msgType.equals("commit")) {
				if (tpcOpIdList.getLength()==0)
					throw new KVException(new KVMessage("resp", "Message format incorrect"));
                
                String tpcOpId = tpcOpIdList.item(0).getTextContent();
                if (tpcOpId==null || tpcOpId.length()==0)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                this.tpcOpId = tpcOpId;
            }
			else if (this.msgType.equals("abort")) {
				if (tpcOpIdList.getLength()==0)
					throw new KVException(new KVMessage("resp", "Message format incorrect"));
                
                String tpcOpId = tpcOpIdList.item(0).getTextContent();
                if (tpcOpId==null || tpcOpId.length()==0)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                this.tpcOpId = tpcOpId;

                if (messageList.getLength()==1) {
                    String message = messageList.item(0).getTextContent();
                    if (message==null || message.length()==0)
                        throw new KVException(new KVMessage("resp", "Message format incorrect"));
                    this.message = message;
                }
			}
            else if (this.msgType.equals("ack")) {
				if (tpcOpIdList.getLength()==0)
					throw new KVException(new KVMessage("resp", "Message format incorrect"));
                
                String tpcOpId = tpcOpIdList.item(0).getTextContent();
                if (tpcOpId==null || tpcOpId.length()==0)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                this.tpcOpId = tpcOpId;
            }
			else if (this.msgType.equals("register")) {
				if (messageList.getLength() == 0)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                
                String message = messageList.item(0).getTextContent();
                if (message==null || message.length()==0)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                this.message = message;
			}
            else if (this.msgType.equals("ignoreNext")) {
                // TODO
            }
			else {
				throw new KVException(new KVMessage("resp", "Message format incorrect"));
			}
		}
		catch (IOException e) {
			throw new KVException(new KVMessage("resp", "Network Error: Could not receive data"));
		}
		catch (ParserConfigurationException e) {
			throw new KVException(new KVMessage("resp", "XML Error: Received unparseable message"));
		}
		catch (SAXException e) {
			throw new KVException(new KVMessage("resp", "XML Error: Received unparseable message"));
		}
		catch (KVException e) {
			throw e;
		}
		catch (Exception e) {
			throw new KVException(new KVMessage("resp", "Unknow Error: " + e.getLocalizedMessage()));
		}
	}
	
	/**
	 * 
	 * @param sock Socket to receive from
	 * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
	 * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
	 * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
	 * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
	 */
	public KVMessage(Socket sock) throws KVException {
		
	}

	/**
	 * 
	 * @param sock Socket to receive from
	 * @param timeout Give up after timeout milliseconds
	 * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
	 * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
	 * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
	 * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
	 */
	public KVMessage(Socket sock, int timeout) throws KVException {
	     // TODO: implement me
	}
	
	/**
	 * Copy constructor
	 * 
	 * @param kvm
	 */
	public KVMessage(KVMessage kvm) {
		this.msgType = kvm.msgType;
		this.key = kvm.key;
		this.value = kvm.value;
		this.message = kvm.message;
		this.tpcOpId = kvm.tpcOpId;
	}

	/**
	 * Generate the XML representation for this message.
	 * @return the XML String
	 * @throws KVException if not enough data is available to generate a valid KV XML message
	 */
	public String toXML() throws KVException {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.newDocument();
			doc.setXmlStandalone(true);
			
			Element rootElem = doc.createElement("KVMessage");
			Element keyElem = doc.createElement("Key");
			Element valueElem = doc.createElement("Value");
			Element messageElem = doc.createElement("Message");
            Element tpcOpIdElem = doc.createElement("TPCOpId");
			
			Node keyText = doc.createTextNode(this.key);
			Node valueText = doc.createTextNode(this.value);
			Node messageText = doc.createTextNode(this.message);
            Node tpcOpIdText = doc.createTextNode(this.tcpOpId);
			
			doc.appendChild(rootElem);
			
			if (this.msgType.equals("getreq")) {
				rootElem.setAttribute("type", "getreq");
				if (this.key==null || this.key.length()==0)
					throw new KVException(new KVMessage("resp", "Message format incorrect"));
				rootElem.appendChild(keyElem);
                rootElem.appendChild(tpcOpIdElem);
				keyElem.appendChild(keyText);
                tpcOpIdElem.appendChild(tpcOpIdText);
                // TODO
			}
			else if (this.msgType.equals("putreq")) {
				rootElem.setAttribute("type", "putreq");
				if (this.key==null || this.key.length()==0 || this.value==null || this.value.length()==0)
					throw new KVException(new KVMessage("resp", "Message format incorrect"));
				rootElem.appendChild(keyElem);
				rootElem.appendChild(valueElem);
                rootElem.appendChild(tpcOpIdElem);
				keyElem.appendChild(keyText);
				valueElem.appendChild(valueText);
                tpcOpIdElem.appendChild(tpcOpIdText);
			}
			else if (this.msgType.equals("delreq")) {
				rootElem.setAttribute("type", "delreq");
				if (this.key==null || this.key.length()==0)
					throw new KVException(new KVMessage("resp", "Message format incorrect"));
				rootElem.appendChild(keyElem);
                rootElem.appendChild(tpcOpIdElem);
				keyElem.appendChild(keyText);
                tpcOpIdElem.appendChild(tpcOpIdText);
			}
			else if (this.msgType.equals("resp")) {
				rootElem.setAttribute("type", "resp");
				if (this.message==null || this.message.length()==0) {
					if (this.key==null || this.key.length()==0 || this.value==null || this.value.length()==0)
						throw new KVException(new KVMessage("resp", "Message format incorrect"));
					rootElem.appendChild(keyElem);
					rootElem.appendChild(valueElem);
					keyElem.appendChild(keyText);
					valueElem.appendChild(valueText);
				}
				else {
					rootElem.appendChild(messageElem);
					messageElem.appendChild(messageText);
				}
			}
            else if (this.msgType.equals("ready")) {
                rootElem.setAttribute("type", "ready");
                if (this.tpcOpId==null)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                rootElem.appendChild(tpcOpIdElem);
                tpcOpIdElem.appendChild(tpcOpIdText);
            }
            else if (this.msgType.equals("commit")) {
                rootElem.setAttribute("type", "commit");
                if (this.tpcOpId==null)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                rootElem.appendChild(tpcOpIdElem);
                tpcOpIdElem.appendChild(tpcOpIdText);
            }
            else if (this.msgType.equals("abort")) {
                rootElem.setAttribute("type", "abort");
                if (this.tpcOpId==null)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                rootElem.appendChild(tpcOpIdElem);
                tpcOpIdElem.appendChild(tpcOpIdText);
                if (this.message!=null) {
                    rootElem.appendChild(messageElem);
                    messageElem.appendChild(messageText);
                }
            }
            else if (this.msgType.equals("ack")) {
                rootElem.setAttribute("type", "ack");
                if (this.tpcOpId==null)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                rootElem.appendChild(tpcOpIdElem);
                tpcOpIdElem.appendChild(tpcOpIdText);
            }
            else if (this.msgType.equals("register")) {
                rootElem.setAttribute("type", "register");
                if (this.message==null || this.message.length()==0)
                    throw new KVException(new KVMessage("resp", "Message format incorrect"));
                rootElem.appendChild(messageElem);
                messageElem.appendChild(messageText);
            }
            else if (this.msgType.equals("ignoreNext")) {
                rootElem.setAttribute("type", "ignoreNext");
                // TODO
            }
			else {
				throw new KVException(new KVMessage("resp", "Message format incorrect"));
			}
			
			DOMSource domSource = new DOMSource(doc);
			StringWriter stringWriter = new StringWriter();
			StreamResult streamResult = new StreamResult(stringWriter);
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			transformer.transform(domSource, streamResult);
			String xmlString = stringWriter.toString();
			
			return xmlString;
		}
		catch (KVException e) {
			throw e;
		}
		catch (Exception e) {
			e.getStackTrace();
		}
		return null;
	}
	
	public void sendMessage(Socket sock) throws KVException {
		try {
			String xml = this.toXML();
			OutputStream os = socket.getOutputStream();
			os.write(xml.getBytes(), 0, xml.length());
		}
		catch (IOException e) {
			throw new KVException(new KVMessage("resp", "Network Error: Could not send data"));
		}
		catch (KVException e) {
			throw e;
		}
		catch (Exception e) {
			throw new KVException(new KVMessage("resp", "Unknown Error: " + e.getLocalizedMessage()));
		}
		
		try {
			socket.getOutputStream().flush();
		}
		catch (IOException e) {
			throw new KVException(new KVMessage("resp", "Unknown Error: Socket is not connected"));
		}
		
		try {
	    	socket.shutdownOutput();
	    }
	    catch (IOException e) {
	    	throw new KVException(new KVMessage("resp", "Unknown Error: " + e.getLocalizedMessage()));
	    }
	}
	
	public void sendMessage(Socket sock, int timeout) throws KVException {
		/*
		 * As was pointed out, setting a timeout when sending the message (while would still technically work),
		 * is a bit silly. As such, this method will be taken out at the end of Spring 2013.
		 */
		// TODO: optional implement me
	}
    /** Part I END */

    //Class
	/* Solution from http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html */
	private class NoCloseInputStream extends FilterInputStream {
	    public NoCloseInputStream(InputStream in) {
	        super(in);
	    }
	    
	    public void close() {} // ignore close
	}
}
