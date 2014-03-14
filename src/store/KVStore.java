package store;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

/*
 * Currently using global lock.
 * TODO: Consider optimizing this.
 */

public class KVStore implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private HashMap<Long,byte[]> mystore;
	private int slice;
	private int nslices;
	private Logger log;
	private HashMap<Long,Boolean> seen;
	private HashMap<String,Boolean> requestlog;
	

	
	public KVStore(Logger log){
		this.log = log;
		this.log.debug("Initializing empty store.");
		this.mystore = new HashMap<Long,byte[]>();
		this.seen = new HashMap<Long,Boolean>();
		this.requestlog = new HashMap<String,Boolean>();
		this.nslices = 0;
		this.slice = 0;
	}

	public void init(){
		this.log.debug("NOT LOADING DATA FROM FILE");
		this.mystore = new HashMap<Long,byte[]>();
		this.seen = new HashMap<Long,Boolean>();
		this.requestlog = new HashMap<String,Boolean>();
		this.nslices = 0;
		this.slice = 0;
	}

	public void writeObject(ObjectOutputStream s){
		try {
			s.writeInt(this.slice);
			s.writeInt(this.nslices);
			s.writeObject(mystore);
			s.writeObject(seen);
		} catch (IOException e) {
			log.error("Error serializing object Store.");
			e.printStackTrace();
		}
	}
	
	public String toString(){
		String s = "[";
		Set<Long> skeys = this.mystore.keySet();
		Iterator<Long> storedkeys = skeys.iterator();
		while(storedkeys.hasNext()){
			Long skey = storedkeys.next();
			s = s + skey + " ";
		}
		s = s + "]";
		return s;
	}
	
	@SuppressWarnings("unchecked")
	public void readObject(ObjectInputStream s){
		try {
			this.slice = s.readInt();
			this.nslices = s.readInt();
			this.mystore = (HashMap<Long,byte[]>) s.readObject();
			this.seen = (HashMap<Long,Boolean>) s.readObject();
			this.requestlog = new HashMap<String,Boolean>();
			log.debug("DATA LOADED \nSlice: "+this.slice+"\nNSlices: "+this.nslices+" KEY:VALUE->"+this.toString());
		} catch (IOException e) {
			log.error("Error reading object Store from file.");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			log.error("Error reading object Store from file.");
			e.printStackTrace();
		}
	}
	
	public int getSliceForKey(long key){
		int res = (int) Math.abs(key%this.nslices);
		if(res==0) res = 1;
		return res;
	}
	
	//Use only outside simulation! - it is not synchronized
	public Long[] getStoredKeys(){
		Long[] tmp = this.mystore.keySet().toArray(new Long[this.mystore.size()]);
		return tmp;
	}
	//------------------------------------------------------
	
	public synchronized void updatePartition(int p, int np){
		this.nslices = np;
		this.slice = p;
		//clear memory to allow new keys to be stored has the slice has changed
		for(Long lk : this.seen.keySet()){
			if(!this.mystore.containsKey(lk)){
				this.seen.put(lk, false);
			}
		}
	}
	
	public synchronized HashSet<Long> getKeys(){
		HashSet<Long> keys = new HashSet<Long>();
		for(Long l : mystore.keySet()){
			keys.add(l);
		}
		return keys;
	}
	
	public synchronized boolean haveseen(Long key){
		Boolean b = this.seen.get(key);
		if(b==null){
			return false;
		}
		else{
			return true;
		}
	}
	
	public synchronized void seenit(Long key){
		this.seen.put(key, true);
	}
	
	public synchronized boolean inLog(String key){
		Boolean b = this.requestlog.get(key);
		if(b==null){
			//this.requestlog.put(key, true);
			return false;
		}
		else{
			return true;
		}
	}
	
	public synchronized void logreq(String key){
		this.requestlog.put(key, true);
	}
	
	public synchronized boolean put(long key, byte[] data) {
		this.seenit(key);
		int sslice = this.getSliceForKey(key);
		if(this.slice==sslice){
			this.mystore.put(key, data);
			log.debug("Object inserted into Store. ("+key+","+data.toString()+")");
			String storecontents = "STORECONTENTS [ ";
			Set<Long> skeys = mystore.keySet();
			Iterator<Long> storedkeys = skeys.iterator();
			while(storedkeys.hasNext()){
				Long skey = storedkeys.next();
				storecontents = storecontents + skey + " ";
			}
			storecontents = storecontents + "]";
			log.info(storecontents);
			return true;
		}
		else{
			log.debug("Object received but does not belong in this Store. ("+key+","+data.toString()+")");
			return false;
		}
	}

	public synchronized byte[] get(long key) {
		byte[] value = this.mystore.get(key);
		return value;
	}

	public synchronized byte[] delete(long key) {
		byte[] value = this.mystore.remove(key);
		return value;
	}


	
	public synchronized int getSlice() {
		return slice;
	}

	public synchronized void setSlice(int slice) {
		this.slice = slice;
	}

	public synchronized int getNslices() {
		return nslices;
	}

	public synchronized void setNslices(int nslices) {
		this.nslices = nslices;
	}
	
}
