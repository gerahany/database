package database2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Hashtable;

public class tuple implements Comparable<tuple>,  Serializable {
	Object primkey;
	Hashtable<String,Object> rowdata;
	public tuple (Object primkey,Hashtable<String, Object> htblColNameValue) {
		this.primkey=primkey;
		this.rowdata=(Hashtable<String, Object>) htblColNameValue.clone();
	}
	
	

	public Object getPrimkey() {
		return primkey;
	}


	public void setPrimkey(String primkey) {
		this.primkey = primkey;
	}


	public Hashtable<String, Object> getRowdata() {
		return rowdata;
	}


	public void setRowdata(Hashtable<String, Object> rowdata) {
		this.rowdata = rowdata;
	}

	

	   @Override
	   public int compareTo(tuple o) {
		    Object pk = this.getPrimkey();
		    Object otherPk = o.getPrimkey();
		    if (!(pk instanceof Comparable) || !(otherPk instanceof Comparable)) {
		        throw new UnsupportedOperationException("Cannot compare non-comparable primary keys");
		    }
		    return ((Comparable) pk).compareTo(otherPk);
		}
	  

	    // TODO override this with something useful
	    @Override
	    public String toString() {
	        Object pk = this.getPrimkey();
	        Class cl = pk.getClass();
	        return cl.cast(pk).toString();
	    }
	   }
