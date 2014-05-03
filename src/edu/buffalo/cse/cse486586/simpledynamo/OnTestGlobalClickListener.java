package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class OnTestGlobalClickListener implements OnClickListener {

	private static final String TAG = OnTestGlobalClickListener.class.getName();
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	private final ContentResolver mContentResolver;
	private final Uri mUri;

	public OnTestGlobalClickListener(TextView _tv, ContentResolver _cr) {
		mContentResolver = _cr;
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	
	@Override
	public void onClick(View v) {
		new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
	}

	private class Task extends AsyncTask<Void, String, Void> {
		@Override
		protected Void doInBackground(Void... params) {
			
						
			
			testQuery();
			
			return null;
		}
		

		private boolean testQuery() {
			try {
					Cursor resultCursor = mContentResolver.query(mUri, null,
							"*", null, null);
					if (resultCursor == null) {
						Log.e(TAG, "Result null");
						throw new Exception();
					}

					int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
					int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
					if (keyIndex == -1 || valueIndex == -1) {
						Log.e(TAG, "Wrong columns");
						resultCursor.close();
						throw new Exception();
					}

					resultCursor.moveToFirst();
	    			while(resultCursor.isAfterLast() == false){
	    				resultCursor.getString(keyIndex);
						resultCursor.getString(valueIndex);
						Log.d(TAG,"Key: "+resultCursor.getString(keyIndex)+" value: "+resultCursor.getString(valueIndex));
						resultCursor.moveToNext();
				    }

					resultCursor.close();
			} catch (Exception e) {
				return false;
			}

			return true;
		}
	}
}
