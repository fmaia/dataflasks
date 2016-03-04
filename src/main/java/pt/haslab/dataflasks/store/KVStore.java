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
package pt.haslab.dataflasks.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public interface KVStore {

	public String toString();
	public void readFromString(String[] data);	
	public int getSliceForKey(long key);
	//Use only outside simulation! - it is not synchronized
	public StoreKey[] getStoredKeys();
	//------------------------------------------------------
	public void updatePartition(int p, int np);
	public HashSet<StoreKey> getKeys();
	
	public boolean haveseen(Long key, Long version);
	public void seenit(Long key, Long version);
	public boolean inLog(String key);
	public void logreq(String key);
	public boolean put(long key, long version, byte[] data);
	public byte[] get(StoreKey key);
	public byte[] delete(long key);
	public int getSlice();
	public void setSlice(int slice);
	public int getNslices();
	public void setNslices(int nslices);
	//Anti-entropy log management
	public void aeLog(String key);
	public boolean aeIsInLog(String req);
	//Dedup specific API
	public HashMap<StoreKey,ArrayList<String>> getHashes();
	public ArrayList<String> getMissingHashes(StoreKey k,ArrayList<String> hashes);
}
