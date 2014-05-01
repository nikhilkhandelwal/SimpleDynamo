package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;

import android.app.Application;
import android.content.Context;
import android.database.MatrixCursor;
import android.telephony.TelephonyManager;

public class SimpleDynamoApplication extends Application {

   
    /*TelephonyManager tel = (TelephonyManager) getSystemService(Context.TELEPHONY_SERVICE);
	String portStr = tel.getLine1Number().substring(
			tel.getLine1Number().length() - 4);
	final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));*/
	  
	//final String myPort="11108";
	 public String myPort;
	 private String successor;
	 private String predecessor;
	public volatile boolean waitingForCursor=true;
	public volatile MatrixCursor cursorFromSuccesor;
 
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
	@Override
	public void onCreate() {
		// TODO Auto-generated method stub
		super.onCreate();
		        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
		        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() -4);
		        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		        String columnName[]={SimpleDynamoProvider.KEY_FIELD, SimpleDynamoProvider.VALUE_FIELD };
		        cursorFromSuccesor = new MatrixCursor(columnName);
	}
	
    
}
