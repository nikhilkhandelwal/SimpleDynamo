package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;

public class Message implements Serializable {
	
	/**
	 * 
	 */
	
	private static final long serialVersionUID = 1L;
	public static final int NODE_JOIN=1;
	public static final int SET_NEIGHBOURS=2;
	public static final int INSERT=3;
	public static final int QUERY=4;
	public static final int QUERY_RESULT=5;
	public static final int QUERY_GLOBAL=6;
	public static final int DELETE_GLOBAL=7;
	public static final int DELETE=8;
	public static final int RECOVERY_DATA=9;
	public static final int RECOVERY_DATA_REPLY=10;
	private int messageType=-1;
	private String value;
	private String key;
	private String successor;
	private String predecessor;
	private String sendToPort;
	private String fromPort;
	private ArrayList<Message> globalQuery;
	
	public ArrayList<Message> getGlobalQuery() {
		return globalQuery;
	}
	public void setGlobalQuery(ArrayList<Message> globalQuery) {
		this.globalQuery = globalQuery;
	}
	public String getFromPort() {
		return fromPort;
	}
	public void setFromPort(String fromPort) {
		this.fromPort = fromPort;
	}
	public String getSendToPort() {
		return sendToPort;
	}
	public void setSendToPort(String sendToPort) {
		this.sendToPort = sendToPort;
	}
	public String getSuccessor() {
		return successor;
	}
	public void setSuccessor(String successor) {
		this.successor = successor;
	}
	public String getPredecessor() {
		return predecessor;
	}
	public void setPredecessor(String predecessor) {
		this.predecessor = predecessor;
	}
	
	public Message() {
		super();
		
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}

	public int getMessageType() {
		return messageType;
	}
	public void setMessageType(int messageType) {
		this.messageType = messageType;
	}
	

}
