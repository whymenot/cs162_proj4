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

import edu.berkeley.cs162.KVException;

import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.net.Socket;
import java.net.SocketException;

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
	
	//Constructors
	/**
	 * @param msgType
	 * @throws KVException of type "resp" with message "Message format incorrect" if msgType is unknown
	 */
    public KVMessage(String msgType) throws KVException {
        this.msgType = this.validMsgType(msgType);
    }
	
    public KVMessage(String msgType, String message) throws KVException {
        this.msgType = this.validMsgType(msgType);
        this.message = message;
    }
	
    /**
     * Parse KVMessage from incoming network connection
     * @param input
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

			this.msgType = root.getAttribute("type");
			NodeList keyList = root.getElementsByTagName("Key"); //null if there is no such tag
			NodeList valueList = root.getElementsByTagName("Value");
			NodeList messageList = root.getElementsByTagName("Message");
            NodeList tpcOpIdList = root.getElementsByTagName("TPCOpId");

            switch (this.msgType) {
                case "getreq":
                    this.key = this.validNodeList(keyList);
                    break;
                case "putreq":
                    this.key = this.validNodeList(keyList);
                    this.value = this.validNodeList(valueList);
                    if (tpcOpIdList.getLength()==1)
                        this.tpcOpId = this.validNodeList(tpcOpIdList);
                    break;
                case "delreq":
                    this.key = this.validNodeList(keyList);
                    if (tpcOpIdList.getLength()==1)
                        this.tpcOpId = this.validNodeList(tpcOpIdList);
                    break;
                case "resp":
                    if (messageList.getLength() == 0) {
                        this.key = this.validNodeList(keyList);
                        this.value = this.validNodeList(valueList);
                    }
                    else
                        this.message = this.validNodeList(messageList);
                    break;
                case "abort":
                	if (messageList.getLength()==1)
                        this.message = this.validNodeList(messageList);
                    this.tpcOpId = this.validNodeList(tpcOpIdList);
                    break;
                case "commit":
                    this.tpcOpId = this.validNodeList(tpcOpIdList);
                    break;
                case "ready":
                    this.tpcOpId = this.validNodeList(tpcOpIdList);
                    break;
                case "ack":
                    this.tpcOpId = this.validNodeList(tpcOpIdList);
                    break;
                case "register":
                    this.message = this.validNodeList(messageList);
                    break;
                case "ignoreNext":
                    // TODO
                    break;
                default:
				    throw new KVException(new KVMessage("resp", "Message format incorrect"));
			}
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
	 * @param socket Socket to receive from
	 * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
	 * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
	 * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
	 * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
	 */
	public KVMessage(Socket socket) throws KVException {
		try {
			InputStream is = socket.getInputStream();
			this(is);
			socket.close();
		}
		catch (IOException e) {
			throw new KVException(new KVMessage("resp", "Network Error: Could not receive data"));
		}
		catch (KVException e) {
			throw e;
		}
	}

	/**
	 * 
	 * @param socket Socket to receive from
	 * @param timeout Give up after timeout milliseconds
	 * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
	 * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
	 * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
	 * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
	 */
	public KVMessage(Socket socket, int timeout) throws KVException {
		try {
			socket.setSoTimeout(timeout);
			this(socket);
		}
		catch (SocketException e) {
			//Do nothing
		}
		catch (KVException e) {
			throw e;
		}
	}
	
	/**
	 * Copy constructor
	 * @param kvm
	 */
	public KVMessage(KVMessage kvm) {
		this.msgType = kvm.msgType;
		this.key = kvm.key;
		this.value = kvm.value;
		this.message = kvm.message;
		this.tpcOpId = kvm.tpcOpId;
	}

	//Helper Methods
	public final String getMsgType() { return msgType; }
	public final String getKey() { return key; }
	public final String getValue() { return value; }
	public final String getMessage() { return message; }
	public final String getTpcOpId() { return tpcOpId; }

    public final void setMsgType(String msgType) { this.msgType = msgType; }
	public final void setKey(String key) { this.key = key; }
	public final void setValue(String value) { this.value = value; }
	public final void setMessage(String message) { this.message = message; }
	public final void setTpcOpId(String tpcOpId) { this.tpcOpId = tpcOpId; }

	private String validMsgType(String msgType) throws KVException {
		if (msgType.equals("getreq") || msgType.equals("putreq") ||
            msgType.equals("delreq") || msgType.equals("resp") ||
            msgType.equals("abort") || msgType.equals("commit") ||
            msgType.equals("ready") || msgType.equals("ack") ||
            msgType.equals("register") || msgType.equals("ignoreNext"))
			return msgType;
        throw new KVException(new KVMessage("resp", "Message format incorrect"));
	}

    public String validNodeList(NodeList nodeList) throws KVException {
        if (nodeList.getLength()==0)
            throw new KVException(new KVMessage("resp", "Message format incorrect"));
        
        String result = nodeList.item(0).getTextContent();
        if (result==null || result.length()==0)
            throw new KVException(new KVMessage("resp", "Message format incorrect"));
        return result;
    }

    public void appendElem(String field, Element parent, Element child, Node text) throws KVException {
        if (field==null || field.length()==0)
            throw new KVException(new KVMessage("resp", "Message format incorrect"));
        parent.appendChild(child);
        child.appendChild(text);
    }

    //Action Methods
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
            Node tpcOpIdText = doc.createTextNode(this.tpcOpId);
			
			doc.appendChild(rootElem);
            switch (this.msgType) {
                case "getreq":
                    rootElem.setAttribute("type", "getreq");
                    this.appendElem(this.key, rootElem, keyElem, keyText);
                    break;
                case "putreq":
                    rootElem.setAttribute("type", "putreq");
                    this.appendElem(this.key, rootElem, keyElem, keyText);
                    this.appendElem(this.value, rootElem, valueElem, valueText);

                    if (this.tpcOpId!=null)
                        this.appendElem(this.tpcOpId, rootElem, tpcOpIdElem, tpcOpIdText);
                    break;
                case "delreq":
                    rootElem.setAttribute("type", "delreq");
                    this.appendElem(this.key, rootElem, keyElem, keyText);

                    if (this.tpcOpId!=null)
                        this.appendElem(this.tpcOpId, rootElem, tpcOpIdElem, tpcOpIdText);
                    break;
                case "resp":
                    rootElem.setAttribute("type", "resp");
                    if (this.message==null) {
                        this.appendElem(this.key, rootElem, keyElem, keyText);
                        this.appendElem(this.value, rootElem, valueElem, valueText);
                    }
                    else
                        this.appendElem(this.message, rootElem, messageElem, messageText);
                    break;
                case "abort":
                    rootElem.setAttribute("type", "abort");
                    if (this.message!=null)
                        this.appendElem(this.message, rootElem, messageElem, messageText);
                    this.appendElem(this.tpcOpId, rootElem, tpcOpIdElem, tpcOpIdText);
                    break;
                case "commit":
                    rootElem.setAttribute("type", "commit");
                    this.appendElem(this.tpcOpId, rootElem, tpcOpIdElem, tpcOpIdText);
                    break;
                case "ready":
                    rootElem.setAttribute("type", "ready");
                    this.appendElem(this.tpcOpId, rootElem, tpcOpIdElem, tpcOpIdText);
                    break;
                case "ack":
                    rootElem.setAttribute("type", "ack");
                    this.appendElem(this.tpcOpId, rootElem, tpcOpIdElem, tpcOpIdText);
                    break;
                case "register":
                    rootElem.setAttribute("type", "register");
                    this.appendElem(this.message, rootElem, messageElem, messageText);
                    break;
                case "ignoreNext":
                    rootElem.setAttribute("type", "ignoreNext");
                    // TODO
                    break;
                default:
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
		catch (KVException e) { throw e; }
		catch (Exception e) { e.getStackTrace(); }
		return null;
	}
	
	public void sendMessage(Socket socket) throws KVException {
		try {
			String xml = this.toXML();
			OutputStream os = socket.getOutputStream();
			os.write(xml.getBytes(), 0, xml.length());
		}
		catch (IOException e) { throw new KVException(new KVMessage("resp", "Network Error: Could not send data")); }
		catch (KVException e) { throw e; }
		catch (Exception e) { throw new KVException(new KVMessage("resp", "Unknown Error: " + e.getLocalizedMessage())); }
		
		try {
			socket.getOutputStream().flush();
		}
		catch (IOException e) { throw new KVException(new KVMessage("resp", "Unknown Error: Socket is not connected")); }
		
		try {
			socket.shutdownOutput();
		}
	    catch (IOException e) { throw new KVException(new KVMessage("resp", "Unknown Error: " + e.getLocalizedMessage())); }
	}
	
	public void sendMessage(Socket socket, int timeout) throws KVException {
		/*
		 * As was pointed out, setting a timeout when sending the message (while would still technically work),
		 * is a bit silly. As such, this method will be taken out at the end of Spring 2013.
		 */
		try {
			socket.setSoTimeout(timeout);
			this.sendMessage(socket);
		}
		catch (SocketException e) {
			//Do nothing
		}
		catch (KVException e) {
			throw e;
		}
	}

    //Class
	/* Solution from http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html */
	private class NoCloseInputStream extends FilterInputStream {
	    public NoCloseInputStream(InputStream in) {
	        super(in);
	    }
	    
	    public void close() {} // ignore close
	}
}
