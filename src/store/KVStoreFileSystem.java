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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import common.DFLogger;

/*
 * Currently using global lock.
 * TODO: Consider optimizing this.
 */

public class KVStoreFileSystem implements KVStore{

	private final String filename = "database";
	private HashMap<StoreKey,StoreFile> mystore;
	private int slice;
	private int nslices;
	//public Logger log;
	private HashMap<StoreKey,Boolean> seen;
	private HashMap<String,Boolean> requestlog;
	private HashMap<String,Boolean> antientropylog;
	
	//FileSystem Info
	private int currentOffset;
	private FileOutputStream f;
	
	public KVStoreFileSystem(DFLogger log){
		//this.log = log;
		this.mystore = new HashMap<StoreKey,StoreFile>();
		this.seen = new HashMap<StoreKey,Boolean>();
		this.requestlog = new HashMap<String,Boolean>();
		this.antientropylog = new HashMap<String,Boolean>();
		this.nslices = 0;
		this.slice = 0;
		
		this.currentOffset = 0;
		try {
			this.f = new FileOutputStream(filename,true);
		} catch (FileNotFoundException e) {
			File nf = new File(filename);
			try {
				nf.createNewFile();
				this.f = new FileOutputStream(filename,true);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
	}
	
	public KVStoreFileSystem(){
		this.mystore = new HashMap<StoreKey,StoreFile>();
		this.seen = new HashMap<StoreKey,Boolean>();
		this.requestlog = new HashMap<String,Boolean>();
		this.antientropylog = new HashMap<String,Boolean>();
		this.nslices = 0;
		this.slice = 0;
	}
	
	@Override
	public String toString(){
		String s = this.slice + "\n" + this.nslices + "\n";
		Set<StoreKey> skeys = this.mystore.keySet();
		s = s + skeys.size() + "\n";
		Iterator<StoreKey> storedkeys = skeys.iterator();
		while(storedkeys.hasNext()){
			StoreKey skey = storedkeys.next();
			s = s + skey.key + "\n" + skey.version + "\n" + this.readData(this.mystore.get(skey)) + "\n";
		}
		s = s + this.seen.size();
		for(StoreKey lo : this.seen.keySet()){
			s = s + "\n" + lo.key + "\n" + lo.version + "\n" + seen.get(lo);
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
				Long version = Long.parseLong(lines[i+1]);
				byte[] v = lines[i+2].getBytes();
				StoreFile nf = this.writeData(k, version, v);
				this.mystore.put(new StoreKey(k,version),nf);
				i = i + 2;
			}
			siz = Integer.parseInt(lines[i]);
			i = i + 1;
			for(int j=0;j<siz;j++){
				Long ls = Long.parseLong(lines[i]);
				Long lsversion = Long.parseLong(lines[i+1]);
				Boolean bs = Boolean.parseBoolean(lines[i+2]);
				this.seen.put(new StoreKey(ls,lsversion), bs);
				i = i + 2;
			}
			
		}
		else{
			System.err.println("KVSTORE ERROR reading data from String.");
		}
	}
	
	public int getSliceForKey(long key){
		//FIX ME - currently compatible with positive long only
		long min = 0;//Long.MIN_VALUE;
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
	public StoreKey[] getStoredKeys(){
		StoreKey[] tmp = this.mystore.keySet().toArray(new StoreKey[0]);
		return tmp;
	}
	//------------------------------------------------------
	
	public synchronized boolean put(long key, long version, byte[] data) {
		StoreKey rec = new StoreKey(key, version);
		this.seenit(key, version);
		int sslice = this.getSliceForKey(key);
		if(this.slice==sslice){
			StoreFile nfile = this.writeData(key, version, data);
			this.mystore.put(rec, nfile);
			return true;
		}
		else{
			//log.debug("Object received but does not belong in this Store. ("+key+","+data.toString()+")");
			return false;
		}
	}

	private StoreFile writeData(long key, long version, byte[] data){
		int datalen = data.length;
		StoreFile rf = new StoreFile();
		rf.fileStart = this.currentOffset;
		rf.size = datalen;
		try {
			this.f.write(data);
		} catch (IOException e) {
			//ERROR WRITING FILE
			e.printStackTrace();
			return null;
		}
		this.currentOffset = this.currentOffset + datalen;
		
		return rf;
	}
	
	public synchronized byte[] get(StoreKey key) {
		StoreFile f = this.mystore.get(key);
		return this.readData(f);
	}
	
	private byte[] readData(StoreFile f){
		try {
			FileInputStream fin = new FileInputStream(filename);
			long skipped = 0;
			while(skipped!=f.fileStart){
				skipped = fin.skip(f.fileStart-skipped);
			}
			byte[] nb = new byte[f.size];
			fin.read(nb);
			fin.close();
			return nb;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public synchronized byte[] delete(long key) {
		//Not implemented
		return null;
	}

	
	public synchronized void updatePartition(int p, int np){
		if(np!=this.nslices){
			this.nslices = np;
			this.slice = p;
			//clear memory to allow new keys to be stored has the slice has changed
			for(StoreKey lk : this.seen.keySet()){
				if(!this.mystore.containsKey(lk)){
					this.seen.put(lk, false);
				}
			}
		}
	}
	
	public synchronized HashSet<StoreKey> getKeys(){
		HashSet<StoreKey> keys = new HashSet<StoreKey>();
		for(StoreKey l : mystore.keySet()){
			keys.add(l);
		}
		return keys;
	}
	
	//Store key log management
	
	public synchronized boolean haveseen(Long key, Long version){
		StoreKey rec = new StoreKey(key, version);
		Boolean b = this.seen.get(rec);
		if(b==null){
			return false;
		}
		else{
			return b;
		}
	}
	
	public synchronized void seenit(Long key, Long version){
		StoreKey rec = new StoreKey(key, version);
		this.seen.put(rec, true);
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
	

	//Group Management
	
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
	
	
	//Anti-entropy log management
	
	public synchronized void aeLog(String key){
		this.antientropylog.put(key, true);
	}
	
	public synchronized boolean aeIsInLog(String req){
		Boolean b = this.antientropylog.get(req);
		if(b==null){
			return false;
		}
		else{
			return b;
		}
	}
	
}
