package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;

import android.app.Activity;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.EditText;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	static final int SERVER_PORT = 10000;
	private static final String TAG = "SimpleDynamoActivity";
	Uri mUri;
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		((SimpleDynamoApplication) this
				.getApplicationContext()).setSuccessor(nodeSuccesor());
		
		  try {
				ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
				serverSocket.setReuseAddress(true);
				Runnable task = new ServerClass(this, getContentResolver(),serverSocket);
				Thread t = new Thread(task);
				t.start();
			} catch (IOException e) {
				/*
				 */
				Log.e(TAG, "Can't create a ServerSocket" + e.toString());
				return;
			}
    
		final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
       */
        findViewById(R.id.button2).setOnClickListener(
                new OnTestLocalClickListener(tv, getContentResolver())); 
       /* findViewById(R.id.button2).setOnClickListener(
                new OnTestGlobalClickListener(tv, getContentResolver()));*/
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
/*        findViewById(R.id.button4).setOnClickListener( new OnClickListener() {
			@Override
			public void onClick(View v) {
				// TODO Auto-generated method stub
				
				getContentResolver().delete(mUri, "@",null);
				
			}
		});*/
      /*  findViewById(R.id.button5).setOnClickListener( new OnClickListener() {
			@Override
			public void onClick(View v) {
				// TODO Auto-generated method stub
				
				getContentResolver().delete(mUri, "*",null);
				
			}
		});
        findViewById(R.id.button2).setOnClickListener( new OnClickListener() {
			@Override
			public void onClick(View v) {
				// TODO Auto-generated method stub
				 final  String text = ((EditText) findViewById(R.id.editText1)).getText().toString();
					
				
				getContentResolver().delete(mUri, text,null);
				
			}
		});*/
        
      
      
 //     Log.d(TAG,"text entered: "+text);
        
        findViewById(R.id.button3).setOnClickListener( new OnClickListener() {
			@Override
			public void onClick(View v) {
				// TODO Auto-generated method stub
				final  String text = ((EditText) findViewById(R.id.editText1)).getText().toString();
				//String tempKey ="key";
				Log.d(TAG,text);
				Cursor cursor=getContentResolver().query(mUri, null,
						text, null, null);
			
				int keyIndex = cursor.getColumnIndex("key");
				int valueIndex = cursor.getColumnIndex("value");

				cursor.moveToFirst();
				String returnKey = cursor.getString(keyIndex);
				String returnValue = cursor.getString(valueIndex);
				
				tv.setText(returnKey+" : "+returnValue);
				
				Log.d("nick",returnKey+" : "+returnValue);

				
			}
		});
        
        findViewById(R.id.button1).setOnClickListener( new OnClickListener() {
			@Override
			public void onClick(View v) {
				// TODO Auto-generated method stub
				ContentValues cv;
			   final  String text = ((EditText) findViewById(R.id.editText1)).getText().toString();
		//		final String text ="key";
				cv = new ContentValues();
				
				cv.put("key", text);
				cv.put("value", text);
				
				getContentResolver().insert(mUri,cv );
			}	});
	
        }

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	
	public String nodeSuccesor() {
		ArrayList<String> nodesList = new ArrayList<String>();
			nodesList.add("5554");
			nodesList.add("5556");
			nodesList.add("5558");
			nodesList.add("5560");
			nodesList.add("5562");
			Collections.sort(nodesList, new GenHashComparator());

			int index = nodesList.indexOf(String.valueOf((Integer.parseInt(((SimpleDynamoApplication) this
										.getApplicationContext()).myPort)/2)));
			return String.valueOf(Integer.parseInt(nodesList.get((index+1)%nodesList.size()))*2);

	}
}
