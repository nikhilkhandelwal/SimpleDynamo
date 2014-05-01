package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import android.util.Log;

public class ClientClass implements Runnable {
	private final Message message;
	private static final String TAG = "ClientTask";
	ClientClass(Message message) {
		this.message = message;
	}

	@Override
	public void run() {
		try {
			Log.d(TAG, "Inside client task"+message.getMessageType());
			Socket socket = null;
			socket = new Socket(InetAddress.getByAddress(new byte[] { 10,
					0, 2, 2 }), Integer.parseInt(message.getSendToPort()));
			if(socket.isConnected())
			{
			ObjectOutputStream outToClient = new ObjectOutputStream(
					socket.getOutputStream());
			outToClient.writeObject(message);
			
			socket.close();
			}
		} catch (IOException e) {
			Log.e(TAG, "ClientTask UnknownHostException");
		} 
		throw new RuntimeException();
	}

	}
