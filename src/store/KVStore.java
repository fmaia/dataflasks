/*
Copyright (c) 2014.

Universidade do Minho
Francisco Maia

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
*/
package store;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

/*
 * Currently using global lock.
 * TODO: Consider optimizing this.
 */

public class KVStore {

	
	private HashMap<Long,byte[]> mystore;
	private int slice;
	private int nslices;
	//public Logger log;
	private HashMap<Long,Boolean> seen;
	private HashMap<String,Boolean> requestlog;
	

	
	public KVStore(Logger log){
		//this.log = log;
		this.mystore = new HashMap<Long,byte[]>();
		this.seen = new HashMap<Long,Boolean>();
		this.requestlog = new HashMap<String,Boolean>();
		this.nslices = 0;
		this.slice = 0;
	}
	
	public KVStore(){
		this.mystore = new HashMap<Long,byte[]>();
		this.seen = new HashMap<Long,Boolean>();
		this.requestlog = new HashMap<String,Boolean>();
		this.nslices = 0;
		this.slice = 0;
	}
	
	@Override
	public String toString(){
		String s = this.slice + "\n" + this.nslices + "\n";
		Set<Long> skeys = this.mystore.keySet();
		s = s + skeys.size() + "\n";
		Iterator<Long> storedkeys = skeys.iterator();
		while(storedkeys.hasNext()){
			Long skey = storedkeys.next();
			s = s + skey + "\n" + this.mystore.get(skey) + "\n";
		}
		s = s + this.seen.size();
		for(Long lo : this.seen.keySet()){
			s = s + "\n" + lo + "\n" + seen.get(lo);
		}
		s = s + "\n";
		return s;
	}
	
	public void readFromString(String[] data){
		String[] lines = data;
		if(lines.length!=0){
			this.slice = Integer.parseInt(lines[0]);
			this.nslices = Integer.parseInt(lines[1]);
			int siz = Integer.parseInt(lines[2]);
			int i = 3;
			for(int j=0;j<siz;j++){
				Long k = Long.parseLong(lines[i]);
				byte[] v = lines[i+1].getBytes();
				this.mystore.put(k,v);
				i = i + 2;
			}
			siz = Integer.parseInt(lines[i]);
			i = i + 1;
			for(int j=0;j<siz;j++){
				Long ls = Long.parseLong(lines[i]);
				Boolean bs = Boolean.parseBoolean(lines[i+1]);
				this.seen.put(ls, bs);
				i = i + 2;
			}
			
		}
		else{
			System.err.println("KVSTORE ERROR reading data from String.");
		}
	}
	
	public int getSliceForKey(long key){
		//FIX ME - currently compatible with positive long only
		long min = 1;//Long.MIN_VALUE;
        long max = Long.MAX_VALUE;
        long step = max/this.nslices; ///2);
        long current = min;
        int res = 0;
        while(key>current){
                current = current + step;
                res = res + 1;
        }
        //Correct possible error because how min and max count the 0 value
        if(res>this.nslices){
                res = this.nslices;
        }
        return res;
	}
	
	//Use only outside simulation! - it is not synchronized
	public Long[] getStoredKeys(){
		Long[] tmp = this.mystore.keySet().toArray(new Long[this.mystore.size()]);
		return tmp;
	}
	//------------------------------------------------------
	
	public synchronized void updatePartition(int p, int np){
		if(np!=this.nslices){
			this.nslices = np;
			this.slice = p;
			//clear memory to allow new keys to be stored has the slice has changed
			for(Long lk : this.seen.keySet()){
				if(!this.mystore.containsKey(lk)){
					this.seen.put(lk, false);
				}
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
			return b;
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
			//log.debug("Object inserted into Store. ("+key+","+data.toString()+")");
			//String storecontents = "STORECONTENTS [ ";
			//Set<Long> skeys = mystore.keySet();
			//Iterator<Long> storedkeys = skeys.iterator();
			//while(storedkeys.hasNext()){
			//	Long skey = storedkeys.next();
			//	storecontents = storecontents + skey + " ";
			//}
			//storecontents = storecontents + "]";
			//log.info(storecontents);
			return true;
		}
		else{
			//log.debug("Object received but does not belong in this Store. ("+key+","+data.toString()+")");
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
