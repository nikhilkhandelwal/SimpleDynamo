package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.util.Log;

public class ServerClass implements Runnable {
	static final int SERVER_PORT = 10000;
	ServerSocket serverSocket;
	private ContentValues mContentValues = null;
	public static final String KEY_FIELD = "key";
	public static final String VALUE_FIELD = "value";
	private final Uri mUri;
	Context context;
	private final ContentResolver mContentResolver;

	private static final String TAG = "ServerClass";

	ServerClass(Context context, ContentResolver cr, ServerSocket s) {
		super();
		this.context = context;
		this.mContentResolver = cr;
		mUri = buildUri("content",
				"edu.buffalo.cse.cse486586.simpledynamo.provider");
		serverSocket = s;
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@SuppressWarnings("resource")
	@Override
	public void run() {
		Socket clientSocket = null;

		// nodes.add("11108");
		Log.d(TAG, "Server task started");

		/*
		 * TODO: Fill in your server code that receives messages and passes them
		 * to onProgressUpdate().
		 */
		while (!Thread.currentThread().isInterrupted()) {
			try {
				clientSocket = serverSocket.accept();
				InputStream is = clientSocket.getInputStream();
				Message obj = null;
				ObjectInputStream ois = new ObjectInputStream(is);
				try {
					obj = (Message) ois.readObject();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				if (obj.getMessageType() == Message.INSERT) {
					Log.d(TAG,
							"insert key in server task, key: " + obj.getKey());
					mContentValues = new ContentValues();
					mContentValues.put(KEY_FIELD, obj.getKey());
					mContentValues.put(VALUE_FIELD, obj.getValue());
					mContentResolver.insert(mUri, mContentValues);
				}
				if (obj.getMessageType() == Message.QUERY) {
					Log.d(TAG, "query in server task, key: " + obj.getKey());
					
					
					
					Thread t = new Thread() {
						Message obj;
						private Thread init(Message temp) {
							obj = temp;
							return this;
						}
					    public void run() {
					    	Cursor resultCursor = mContentResolver.query(mUri, null,
									obj.getKey(), null, "DES");
					    	
							while(resultCursor.getCount() < 1)
							{
								Log.d(TAG,"waiting for result");
								resultCursor = mContentResolver.query(mUri, null,
										obj.getKey(), null, "DES");
							}
							System.out.println("cursor size: "
									+ resultCursor.getCount() + " from port "
									+ obj.getFromPort());
							String returnValue = "";

							int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
							//if (resultCursor.getCount() > 0) {
								resultCursor.moveToFirst();
								returnValue = resultCursor.getString(valueIndex);
								Log.d(TAG,
										"Returning the result to: " + obj.getFromPort()
												+ "value: " + returnValue);
							//}
							Log.d(TAG,
									"Returning the result to: " + obj.getFromPort()
											+ "value: " + returnValue);
							final Message msgToSend = new Message();
							msgToSend.setMessageType(Message.QUERY_RESULT);
							msgToSend.setKey(obj.getKey());
							msgToSend.setValue(returnValue);
							msgToSend.setFromPort(obj.getFromPort());
							Thread t = new Thread() {
								private Message msgToSend;

								private Thread init(Message temp) {
									msgToSend = temp;
									return this;
								}

								public void run() {
									try {
										Log.d(TAG, "");
										Socket socket = null;
										socket = new Socket(
												InetAddress.getByAddress(new byte[] {
														10, 0, 2, 2 }),
												Integer.parseInt(msgToSend
														.getFromPort()));
										if (socket.isConnected()) {
											ObjectOutputStream outToClient = new ObjectOutputStream(
													socket.getOutputStream());
											outToClient.writeObject(msgToSend);
											socket.close();
										}
									} catch (UnknownHostException e) {
										Log.e(TAG, "ClientTask UnknownHostException");
									} catch (IOException e) {
										Log.e(TAG,
												"ClientTask socket IOException"
														+ e.toString());
									}
								}
							}.init(msgToSend);
							t.start();
							resultCursor.close();
					    }
					}.init(obj);
					
					t.start();
					
				}
				if (obj.getMessageType() == Message.QUERY_RESULT) {
					Log.d(TAG, "Recieved the query result: " + obj.getKey());
					String[] temp = { obj.getKey(), obj.getValue() };
					String columnName[] = { SimpleDynamoProvider.KEY_FIELD,
							SimpleDynamoProvider.VALUE_FIELD };
					((SimpleDynamoApplication) context.getApplicationContext()).cursorFromSuccesor = new MatrixCursor(
							columnName);

					((SimpleDynamoApplication) context.getApplicationContext()).cursorFromSuccesor
							.addRow(temp);
					((SimpleDynamoApplication) context.getApplicationContext()).waitingForCursor = false;
				}
				if (obj.getMessageType() == Message.QUERY_GLOBAL) {
					ArrayList<Message> globalDump = obj.getGlobalQuery();
					Log.d(TAG, "global query in server task, key: ");
					Cursor resultCursor = mContentResolver.query(mUri, null,
							"@", null, "DES");
					if (obj.getFromPort().compareTo(
							((SimpleDynamoApplication) context
									.getApplicationContext()).myPort) != 0) {
						int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
						int valueIndex = resultCursor
								.getColumnIndex(VALUE_FIELD);
						resultCursor.moveToFirst();
						while (resultCursor.isAfterLast() == false) {
							Message msg = new Message();
							msg.setKey(resultCursor.getString(keyIndex));
							msg.setValue(resultCursor.getString(valueIndex));
							globalDump.add(msg);
							resultCursor.moveToNext();
						}
						final Message globalQuery = new Message();
						globalQuery.setGlobalQuery(globalDump);
						globalQuery.setMessageType(Message.QUERY_GLOBAL);
						globalQuery.setFromPort(obj.getFromPort());
						globalQuery
								.setSendToPort(((SimpleDynamoApplication) context
										.getApplicationContext())
										.getSuccessor());
						Thread t = new Thread() {
							public void run() {
								try {
									Log.d(TAG,
											"Global Query from: "
													+ ((SimpleDynamoApplication) context
															.getApplicationContext()).myPort
													+ " to: "
													+ ((SimpleDynamoApplication) context
															.getApplicationContext())
															.getSuccessor());
									Socket socket = null;
									socket = new Socket(
											InetAddress.getByAddress(new byte[] {
													10, 0, 2, 2 }),
											Integer.parseInt(((SimpleDynamoApplication) context
													.getApplicationContext())
													.getSuccessor()));
									if (socket.isConnected()) {
										ObjectOutputStream outToClient = new ObjectOutputStream(
												socket.getOutputStream());
										outToClient.writeObject(globalQuery);
										socket.close();
									}
								} catch (UnknownHostException e) {
									Log.e(TAG,
											"ClientTask UnknownHostException");
								} catch (IOException e) {
									Log.e(TAG, "ClientTask socket IOException"
											+ e.toString());
								}
							}
						};
						t.start();
						resultCursor.close();
					} else {
						Log.d(TAG,
								"global query updating cursor at "
										+ ((SimpleDynamoApplication) context
												.getApplicationContext()).myPort);
						ArrayList<Message> result = obj.getGlobalQuery();

						String columnName[] = { SimpleDynamoProvider.KEY_FIELD,
								SimpleDynamoProvider.VALUE_FIELD };
						((SimpleDynamoApplication) context
								.getApplicationContext()).cursorFromSuccesor = new MatrixCursor(
								columnName);

						for (Message msg : result) {
							String[] temp = { msg.getKey(), msg.getValue() };
							((SimpleDynamoApplication) context
									.getApplicationContext()).cursorFromSuccesor
									.addRow(temp);
						}

						((SimpleDynamoApplication) context
								.getApplicationContext()).waitingForCursor = false;
					}
				}
				if (obj.getMessageType() == Message.DELETE
						&& obj.getFromPort().compareTo(
								((SimpleDynamoApplication) context
										.getApplicationContext()).myPort) != 0) {
					Log.d(TAG,
							"Delete for key: "
									+ obj.getKey()
									+ " at"
									+ ((SimpleDynamoApplication) context
											.getApplicationContext()).myPort);
					String [] temp = {"deletehere"};
					mContentResolver.delete(mUri, obj.getKey(),temp);
				}
				if (obj.getMessageType() == Message.DELETE_GLOBAL
						&& obj.getFromPort().compareTo(
								((SimpleDynamoApplication) context
										.getApplicationContext()).myPort) != 0) {
					Log.d(TAG,
							"Delete global at"
									+ ((SimpleDynamoApplication) context
											.getApplicationContext()).myPort);
					mContentResolver.delete(mUri, "@", null);
				}

				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
