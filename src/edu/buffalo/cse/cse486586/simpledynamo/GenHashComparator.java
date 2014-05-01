package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Formatter;

public class GenHashComparator implements Comparator<String>{
	

	    @Override
	    public int compare(String o1, String o2) {
		        try {
					if(genHash(o1).compareTo(genHash(o2))> 0)
						return 1; 
					else if(genHash(o1).compareTo(genHash(o2))< 0) 
					return -1;
				} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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

}
