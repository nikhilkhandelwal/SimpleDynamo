package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String TAG = SimpleDynamoProvider.class.getName();
	public static final String KEY_FIELD = "key";
	public static final String VALUE_FIELD = "value";
	public static final String DB_NAME = "simpledht.db";
	public static final String TABLE_NAME = "messages";
	public static final int DB_VERSION = 96;
	private final Uri mUri = buildUri("content",
			"edu.buffalo.cse.cse486586.simpledynamo.provider");
	private DBHelper dbHelper;
	private SQLiteDatabase db;
	ArrayList<String> nodesList = new ArrayList<String>();

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String [] temp = {selection};
		db = dbHelper.getReadableDatabase();
    	
    	if(selection.compareTo("*")==0 || selection.compareTo("@")==0)
    	{
    		if(selection.compareTo("@")==0 )
    		{
    			db.delete(TABLE_NAME,null,null);
    		}
    		if(selection.compareTo("*")==0 )
    		{
    			Log.d(TAG, "* Delete at : "+((SimpleDynamoApplication) getContext()
						.getApplicationContext()).myPort);
    			db.delete(TABLE_NAME, null, null);
    			final Message msgToSend = new Message();
    			msgToSend.setMessageType(Message.DELETE_GLOBAL);
    			msgToSend.setKey("");
    			msgToSend.setValue("");
    			msgToSend.setFromPort(((SimpleDynamoApplication) getContext()
    						.getApplicationContext()).myPort );
    				Thread t = new Thread() {
    				    public void run() {
    				    	try {
    							Log.d(TAG, "Routing delete to next machine");
    							Socket socket = null;
    							socket = new Socket(InetAddress.getByAddress(new byte[] { 10,
    									0, 2, 2 }), Integer.parseInt(((SimpleDynamoApplication) getContext()
    											.getApplicationContext()).getSuccessor()));
    							if(socket.isConnected())
    							{
    							ObjectOutputStream outToClient = new ObjectOutputStream(
    									socket.getOutputStream());
    							outToClient.writeObject(msgToSend);
    							socket.close();
    							}
    						} catch (UnknownHostException e) {
    							Log.e(TAG, "ClientTask UnknownHostException");
    						} catch (IOException e) {
    							Log.e(TAG, "ClientTask socket IOException" + e.toString());
    						}
    				    	
    				    }
    				};
    				t.start();
    		}
    	}
    	else
    	{
    		if(null!=selectionArgs)
    		{
    			Log.d(TAG, "Delete key :"+selection+" at : "+((SimpleDynamoApplication) getContext()
    					.getApplicationContext()).myPort);
    		long id=db.delete(TABLE_NAME, "key=?", temp);
    		Log.d(TAG, "id returned by Delete"+id);
    		}
    		else
    		{
    			String primaryNode = correctNodePort(selection);
        		deleteReplicatedMessage(primaryNode, selection);
    		}
    		
    	}
   
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public String correctNodePort(String keyToStore) {
		String primaryNode = "";
		try {

			for (int indexCurrent = 0; indexCurrent < nodesList.size(); indexCurrent++) {
				int indexPrevious = indexCurrent - 1;
				if (indexPrevious < 0)
					indexPrevious = nodesList.size() + indexPrevious;
				int indexNext = indexCurrent + 1;
				indexNext = indexNext % nodesList.size();

				String port = genHash(nodesList.get(indexCurrent));
				String predecessorPort = genHash(nodesList.get(indexPrevious));
				String successorPort = genHash(nodesList.get(indexNext));

				String key = genHash(keyToStore);
				if (port.compareTo(key) == 0
						|| (key.compareTo(predecessorPort) > 0 && key
								.compareTo(port) <= 0)
						|| (port.compareTo(predecessorPort) < 0 && key
								.compareTo(predecessorPort) > 0)
						|| port.compareTo(successorPort) == 0
						|| (key.compareTo(port) < 0 && port
								.compareTo(predecessorPort) < 0)) {

					primaryNode = nodesList.get(indexCurrent);
					Log.d(TAG, "Node decided for key: "+primaryNode);
					break;

				}
			}
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return String.valueOf(Integer.parseInt(primaryNode)*2);

	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		long id = 0;
		String value = values.getAsString(VALUE_FIELD);
		String primaryNode = "";
		if (!value.contains(":")) {
			//find out the node to store the key
			primaryNode = correctNodePort(values.getAsString(KEY_FIELD));
			//send the key to those nodes to get stored and replication
			id = replicateMessage(primaryNode, values);
		} else {
			Log.d(TAG, "inserting key: " + values.getAsString(KEY_FIELD)
					+ " at" + primaryNode);
			String[] temp = values.getAsString(VALUE_FIELD).split(":");
			
			values.put(VALUE_FIELD, temp[0]);
			db = dbHelper.getWritableDatabase();
			id = db.insertWithOnConflict(TABLE_NAME, null, values,
					SQLiteDatabase.CONFLICT_REPLACE);
			Log.d(TAG, "inserting id: " + id);
		}

		if (id != -1) {

			return Uri.withAppendedPath(uri, Long.toString(id));
		} else {
			return uri;
		}
	}

	private long replicateMessage(String primaryNode, ContentValues values) {
		// TODO Auto-generated method stub
		long id = 0;
		int index = nodesList.indexOf(String.valueOf((Integer.parseInt(primaryNode)/2)));
		for (int i = 0; i < 3; i++) {
			Log.d(TAG,"sent to port: "+String.valueOf(Integer.parseInt(nodesList.get(index % nodesList.size()))*2)+"index: "+index);
			Message msgToSend = new Message();
			msgToSend.setMessageType(Message.INSERT);
			msgToSend.setKey(values.getAsString(KEY_FIELD));
			msgToSend.setValue(values.getAsString(VALUE_FIELD) + ":");
			msgToSend.setFromPort(((SimpleDynamoApplication) getContext()
					.getApplicationContext()).myPort);
			msgToSend.setSendToPort(String.valueOf(Integer.parseInt(nodesList.get(index % nodesList.size()))*2));
			index++;
			Runnable task = new ClientClass(msgToSend);
			Thread t = new Thread(task);
			t.start();
		}
		return id;

	}
	
	private long deleteReplicatedMessage(String primaryNode, String values) {
		// TODO Auto-generated method stub
		long id = 0;
		int index = nodesList.indexOf(String.valueOf((Integer.parseInt(primaryNode)/2)));
		for (int i = 0; i < 3; i++) {
			Log.d(TAG,"sent to port: "+String.valueOf(Integer.parseInt(nodesList.get(index % nodesList.size()))*2)+"index: "+index);
			Message msgToSend = new Message();
			msgToSend.setMessageType(Message.DELETE);
			msgToSend.setKey(values);
			msgToSend.setValue("");
			msgToSend.setFromPort(((SimpleDynamoApplication) getContext()
					.getApplicationContext()).myPort);
			msgToSend.setSendToPort(String.valueOf(Integer.parseInt(nodesList.get(index % nodesList.size()))*2));
			index++;
			Runnable task = new ClientClass(msgToSend);
			Thread t = new Thread(task);
			t.start();
		}
		return id;

	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		dbHelper = new DBHelper(getContext());
		nodesList.add("5554");
		nodesList.add("5556");
		nodesList.add("5558");
		nodesList.add("5560");
		nodesList.add("5562");
		Collections.sort(nodesList, new GenHashComparator());

		return false;
	}

	@SuppressWarnings("resource")
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Cursor cursor;
		Log.d(TAG, "inside query : " + selection);
		final String[] temp = { selection };
		db = dbHelper.getReadableDatabase();
		if (selection.compareTo("@") == 0) {
			// cursor=db.query(TABLE_NAME, projection, null, null , null, null,
			// null);
			cursor = db.query(TABLE_NAME, null, null, null, null, null, null);

			return cursor;
		}
		if (selection.compareTo("*") == 0) {
			ArrayList<Message> globalDump = new ArrayList<Message>();
			cursor = db.query(TABLE_NAME, projection, null, null, null, null,
					null);
			int keyIndex = cursor.getColumnIndex(KEY_FIELD);
			int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
			cursor.moveToFirst();
			while (cursor.isAfterLast() == false) {
				Message msg = new Message();
				msg.setKey(cursor.getString(keyIndex));
				msg.setValue(cursor.getString(valueIndex));
				globalDump.add(msg);
				cursor.moveToNext();
			}
			final Message globalQuery = new Message();
			globalQuery.setGlobalQuery(globalDump);
			globalQuery.setMessageType(Message.QUERY_GLOBAL);
			globalQuery.setFromPort(((SimpleDynamoApplication) getContext()
					.getApplicationContext()).myPort);
			globalQuery.setSendToPort(((SimpleDynamoApplication) getContext()
					.getApplicationContext()).getSuccessor());
			Thread t = new Thread() {
				public void run() {
					try {
						Log.d(TAG, "Routing Query to next machine");
						Socket socket = null;
						socket = new Socket(
								InetAddress.getByAddress(new byte[] { 10, 0, 2,
										2 }),
								Integer.parseInt(((SimpleDynamoApplication) getContext()
										.getApplicationContext())
										.getSuccessor()));
						if (socket.isConnected()) {
							ObjectOutputStream outToClient = new ObjectOutputStream(
									socket.getOutputStream());
							outToClient.writeObject(globalQuery);
							socket.close();
						}
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG,
								"ClientTask socket IOException" + e.toString());
					}
					while (((SimpleDynamoApplication) getContext()
							.getApplicationContext()).waitingForCursor) {
						Log.d(TAG, "In while loop");
						try {
							sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

				}
			};
			t.start();
			try {
				t.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Log.d(TAG, "In updaing global cursor");
			((SimpleDynamoApplication) getContext().getApplicationContext()).waitingForCursor = true;
			return ((SimpleDynamoApplication) getContext().getApplicationContext()).cursorFromSuccesor;
		}

		cursor = db.query(TABLE_NAME, projection, "key=?", temp, null, null,
				null);

		if (sortOrder == null && cursor.getCount() < 1) {
	//	if(!correctNodePort(temp[0]).equals(((SimpleDynamoApplication) getContext().getApplicationContext()).myPort)){
			final Message msgToSend = new Message();
			msgToSend.setMessageType(Message.QUERY);
			msgToSend.setKey(selection);
			msgToSend.setValue("");
			msgToSend.setFromPort(((SimpleDynamoApplication) getContext()
					.getApplicationContext()).myPort);
			Thread t = new Thread() {
				public void run() {
					try {
						Log.d(TAG, "Routing Query to another machine");
						Socket socket = null;
						socket = new Socket(
								InetAddress.getByAddress(new byte[] { 10, 0, 2,
										2 }),Integer.parseInt(correctNodePort(temp[0])));
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
								"ClientTask socket IOException" + e.toString());
					}
					int counter =0;
							((SimpleDynamoApplication) getContext().getApplicationContext()).waitingForCursor = true;
							while (((SimpleDynamoApplication) getContext().getApplicationContext()).waitingForCursor&& counter<100) {
								Log.d(TAG, "In while loop");
								counter++;
								try {
									Thread.sleep(100);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
				             }
							
				}
			};
			t.start();
			try {
				t.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            Log.d(TAG, "In updating gobal cursor");
			((SimpleDynamoApplication) getContext().getApplicationContext()).waitingForCursor = true;
			return ((SimpleDynamoApplication) getContext().getApplicationContext()).cursorFromSuccesor;
		} else {
			Log.d(TAG, "returning local cursor ");
					
					cursor=db.query(TABLE_NAME, projection, "key=?", temp, null, null,
							null);
					
			Log.d(TAG, "recieved result");
			return cursor;

		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	class DBHelper extends SQLiteOpenHelper {

		static final String TAG = "DBHelper";

		public DBHelper(Context context) {
			super(context, SimpleDynamoProvider.DB_NAME, null,
					SimpleDynamoProvider.DB_VERSION);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			// TODO Auto-generated method stub

			String sql = String.format("create table %s "
					+ "(%s text primary key, %s text)",
					SimpleDynamoProvider.TABLE_NAME,
					SimpleDynamoProvider.KEY_FIELD,
					SimpleDynamoProvider.VALUE_FIELD);
			Log.d(TAG, "On create DB Helper");

			db.execSQL(sql);

		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			// TODO Auto-generated method stub
			db.execSQL("drop table if exists "
					+ SimpleDynamoProvider.TABLE_NAME);
			onCreate(db);
		}

	}
}
