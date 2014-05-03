package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
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
	static ArrayList<Message> result;
	static ArrayList<Message> recoveryResult;
	static int recoveryQueryIterator = 0;
	static volatile int globalQueryIteration = 4;
	static int globalQueryIterator = 0;
	private static final String TAG = "ServerClass";
	static boolean flag;

	ServerClass(Context context, ContentResolver cr, ServerSocket s) {
		super();
		this.context = context;
		this.mContentResolver = cr;
		mUri = buildUri("content",
				"edu.buffalo.cse.cse486586.simpledynamo.provider");
		serverSocket = s;
		result = new ArrayList<Message>();
		recoveryResult = new ArrayList<Message>();
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
//		Thread t1 = new Thread() {
//			Message obj;
//
//			private Thread init(Message temp) {
//				obj = temp;
//				return this;
//			}
//
//			public void run() {
//				Cursor resultCursor = mContentResolver.query(mUri,
//						null, obj.getKey(), null, "DES");
//			}};
//		
		recoverData();
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
					try {
						Thread.sleep(20);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					Thread t = new Thread() {
						Message obj;

						private Thread init(Message temp) {
							obj = temp;
							return this;
						}

						public void run() {
							Cursor resultCursor = mContentResolver.query(mUri,
									null, obj.getKey(), null, "DES");

							while (resultCursor.getCount() < 1) {
								Log.d(TAG, "waiting for result");
								resultCursor = mContentResolver.query(mUri,
										null, obj.getKey(), null, "DES");
							}
							System.out.println("cursor size: "
									+ resultCursor.getCount() + " from port "
									+ obj.getFromPort());
							String returnValue = "";

							int valueIndex = resultCursor
									.getColumnIndex(VALUE_FIELD);
							// if (resultCursor.getCount() > 0) {
							resultCursor.moveToFirst();
							returnValue = resultCursor.getString(valueIndex);
							Log.d(TAG,
									"Returning the result to: "
											+ obj.getFromPort() + "value: "
											+ returnValue);
							// }
							/*
							 * Log.d(TAG, "Returning the result to: " +
							 * obj.getFromPort() + "value: " + returnValue);
							 */final Message msgToSend = new Message();
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
										Log.e(TAG,
												"ClientTask UnknownHostException");
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
					ArrayList<Message> globalDump = new ArrayList<Message>();
					// Log.d(TAG, "global query in server task, key: ");
					if (obj.getFromPort().compareTo(
							((SimpleDynamoApplication) context
									.getApplicationContext()).myPort) != 0) {
						Cursor resultCursor = mContentResolver.query(mUri,
								null, "@", null, "DES");

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
						globalQuery.setSendToPort(obj.getFromPort());
						/*
						 * Log.d(TAG, "Global Query from: " +
						 * ((SimpleDynamoApplication) context
						 * .getApplicationContext()).myPort + " to: " +
						 * ((SimpleDynamoApplication) context
						 * .getApplicationContext()));
						 */Runnable task = new ClientClass(globalQuery);
						Thread t = new Thread(task);
						t.start();
						resultCursor.close();
					} else {
						/*
						 * Log.d(TAG, "global query updating cursor at " +
						 * ((SimpleDynamoApplication) context
						 * .getApplicationContext()).myPort);
						 */ArrayList<Message> tempBuffer = new ArrayList<Message>();
						tempBuffer = obj.getGlobalQuery();
						if (tempBuffer.size() > 0) {
							for (Message temp : tempBuffer) {
								result.add(temp);
							}
						}
						globalQueryIterator++;
						if (globalQueryIterator == globalQueryIteration) {
							{
								Cursor resultCursor = mContentResolver.query(
										mUri, null, "@", null, "DES");

								int keyIndex = resultCursor
										.getColumnIndex(KEY_FIELD);
								int valueIndex = resultCursor
										.getColumnIndex(VALUE_FIELD);
								resultCursor.moveToFirst();
								while (resultCursor.isAfterLast() == false) {
									Message msg = new Message();
									msg.setKey(resultCursor.getString(keyIndex));
									msg.setValue(resultCursor
											.getString(valueIndex));
									result.add(msg);
									resultCursor.moveToNext();
								}

								String columnName[] = {
										SimpleDynamoProvider.KEY_FIELD,
										SimpleDynamoProvider.VALUE_FIELD };
								((SimpleDynamoApplication) context
										.getApplicationContext()).cursorFromSuccesor = new MatrixCursor(
										columnName);

								for (Message msg : result) {
									String[] temp = { msg.getKey(),
											msg.getValue() };
									((SimpleDynamoApplication) context
											.getApplicationContext()).cursorFromSuccesor
											.addRow(temp);
								}

								((SimpleDynamoApplication) context
										.getApplicationContext()).waitingForCursor = false;
							}
							globalQueryIteration = 4;
							globalQueryIterator = 0;
							result.clear();
						}
					}
				}
				if (obj.getMessageType() == Message.DELETE){
					//	&& obj.getFromPort().compareTo(
						//		((SimpleDynamoApplication) context
							//			.getApplicationContext()).myPort) != 0) {
					Log.d(TAG,
							"Delete for key: "
									+ obj.getKey()
									+ " at"
									+ ((SimpleDynamoApplication) context
											.getApplicationContext()).myPort);
					String[] temp = { "deletehere" };
				//	mContentResolver.delete(mUri, obj.getKey(), temp);
					String [] temp1 = { obj.getKey() };
					long id=SimpleDynamoProvider.readableDb.delete(SimpleDynamoProvider.TABLE_NAME, "key=?", temp1);
				}
				if (obj.getMessageType() == Message.DELETE_GLOBAL)
						/*&& obj.getFromPort().compareTo(
								((SimpleDynamoApplication) context
										.getApplicationContext()).myPort) != 0)*/ {
					Log.d(TAG,
							"Delete global at"
									+ ((SimpleDynamoApplication) context
											.getApplicationContext()).myPort);
					//mContentResolver.delete(mUri, "@", null);
				  SimpleDynamoProvider.writableDb.delete(SimpleDynamoProvider.TABLE_NAME,null,null);
				}
				if (obj.getMessageType() == Message.RECOVERY_DATA) {
					ArrayList<Message> globalDump = new ArrayList<Message>();
					Log.d(TAG, "recover in server task, key: ");
					Cursor resultCursor = mContentResolver.query(mUri, null,
							"@", null, "DES");

					int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
					int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
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
					globalQuery.setMessageType(Message.RECOVERY_DATA_REPLY);
					globalQuery.setFromPort(obj.getFromPort());
					globalQuery.setSendToPort(obj.getFromPort());
					Log.d(TAG,
							"recover Query from: "
									+ ((SimpleDynamoApplication) context
											.getApplicationContext()).myPort
									+ " to: "
									+ obj.getFromPort());
					Runnable task = new ClientClass(globalQuery);
					Thread t = new Thread(task);
					t.start();
					resultCursor.close();

				}
				if (obj.getMessageType() == Message.RECOVERY_DATA_REPLY) {
					Log.d(TAG,
							"recover data reply at "
									+ ((SimpleDynamoApplication) context
											.getApplicationContext()).myPort);
					ArrayList<Message> tempBuffer = new ArrayList<Message>();
					tempBuffer = obj.getGlobalQuery();
					if (null != tempBuffer) {
						for (Message temp : tempBuffer)
							recoveryResult.add(temp);
					}
					else
					{
						Log.d(TAG,"null was returned during recovery");
					}
					
						recoveryQueryIterator++;
					
					
				//	if (recoveryQueryIterator == 4) {
						
						for (Message msg : recoveryResult) {

								String key = msg.getKey();
								String value = msg.getValue();

								if (checkIfCorrectKey(key)) {
									ContentValues cv = new ContentValues();
									cv.put(KEY_FIELD, key);
									cv.put(VALUE_FIELD, value);
									//mContentResolver.insert(mUri, cv);
									
									long id = SimpleDynamoProvider.writableDb.insertWithOnConflict(SimpleDynamoProvider.TABLE_NAME, null, cv,
											SQLiteDatabase.CONFLICT_REPLACE);
									
									Log.d(TAG,
											"recover data inserted at "
													+ ((SimpleDynamoApplication) context
															.getApplicationContext()).myPort
													+ " " + key);

								}
								else
								{
									Log.d(TAG,
											"recover data rejected at "
													+ ((SimpleDynamoApplication) context
															.getApplicationContext()).myPort
													+ " " + key);

								}

							}

						
						flag = true;
						recoveryQueryIterator = 0;
						recoveryResult.clear();
					//}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void recoverData() {
		// TODO Auto-generated method stub

		Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
			public void uncaughtException(Thread th, Throwable ex) {
				System.out.println("Caught exception in recover Data " + ex);
			}
		};
		Log.d(TAG,
				"inside recover data for "
						+ ((SimpleDynamoApplication) context
								.getApplicationContext()).myPort);
		for (String nodes : SimpleDynamoProvider.nodesList) {
			
			String nodeValue = String.valueOf(Integer.parseInt(nodes) * 2);
			
			if (!nodeValue.equals(((SimpleDynamoApplication) context
					.getApplicationContext()).myPort)) {
				
				Log.d(TAG,
						"sending recovery request to "
								+ nodeValue
								+ "from"
								+ ((SimpleDynamoApplication) context
										.getApplicationContext()).myPort);
				
				Message msgToSend = new Message();
				msgToSend.setMessageType(Message.RECOVERY_DATA);
				msgToSend.setKey("");
				msgToSend.setValue("");
				msgToSend.setFromPort(((SimpleDynamoApplication) context
						.getApplicationContext()).myPort);
				msgToSend
						.setSendToPort(String.valueOf(Integer.parseInt(nodes) * 2));

				Runnable task = new ClientClass(msgToSend);
				Thread t = new Thread(task);
				t.start();
				t.setUncaughtExceptionHandler(h);
			}
		}		
	}

	private boolean checkIfCorrectKey(String key) {
		// TODO Auto-generated method stub
		Log.d(TAG, "Checking for correct n0de");

		String nodeForKey = String.valueOf(Integer
				.parseInt(SimpleDynamoProvider.correctNodePort(key)) / 2);
		Log.d(TAG, "correct node for key: " + nodeForKey);
		int indexOfNodeForKey = SimpleDynamoProvider.nodesList
				.indexOf(nodeForKey);

		int currentNode = SimpleDynamoProvider.nodesList.indexOf(String.valueOf(Integer
				.parseInt(((SimpleDynamoApplication) context
						.getApplicationContext()).myPort)/2));
		
		/*Log.d(TAG,"current node: "+Integer
				.parseInt(((SimpleDynamoApplication) context
						.getApplicationContext()).myPort)/2);
		
		
		Log.d(TAG,"current node: "+currentNode);*/
		
		int predesesorNode = currentNode-1;
		
		if (predesesorNode < 0)
			predesesorNode = SimpleDynamoProvider.nodesList.size()
					+ predesesorNode;
		int nextpredesesorNode = predesesorNode-1;
		if (nextpredesesorNode < 0)
			nextpredesesorNode = SimpleDynamoProvider.nodesList.size()
					+ nextpredesesorNode;

		if (indexOfNodeForKey == currentNode
				|| indexOfNodeForKey == predesesorNode
				|| indexOfNodeForKey == nextpredesesorNode)
			return true;
		else
		{
			Log.d(TAG," rejected because: correct key= "+indexOfNodeForKey+"current node key= "+currentNode);
			return false;
		}
	}

}
