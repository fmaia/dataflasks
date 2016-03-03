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
package pt.haslab.dataflasks.handler;

import java.io.*;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.HashMap;
import pt.haslab.dataflasks.store.StoreKey;

public class Message {

	public int messagetype;
	public String ip;
	public int port;
	public long key;
	public long version;
	public byte[] value;
	public String reqid;
	public long id;
	public HashSet<StoreKey> keys;
	public HashMap<StoreKey,ArrayList<String>> hashlist;
	
	public Message(int type, String ip, int port,long id,String reqid){
		this.messagetype = type;
		this.id = id;
		this.ip = ip;
		this.port = port;
		this.key = 0;
		this.version = 0;
		this.value = null;
		this.reqid = reqid;
	}
	
	public Message(int type, String ip, int port,long id, HashSet<StoreKey> keys, HashMap<StoreKey,ArrayList<String>> hashes ){
		this.messagetype = type;
		this.id = id;
		this.ip = ip;
		this.port = port;
		this.key = 0;
		this.version = 0;
		this.value = null;
		this.reqid = "-1";
		this.keys = keys;
		this.hashlist = hashes;
	}
	
	public Message(int type, String ip, int port,byte[] value,String reqid,Long key,Long version, long id){
		this.messagetype = type;
		this.id = id;
		this.ip = ip;
		this.port = port;
		this.key = key;
		this.version = version;
		this.value = value;
		this.reqid = reqid;
	}
	
	public Message(byte[] packet){
		//initializing values that do not exist for all types of messages
		//this.value = null;
		//this.reqid = "-1";
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(packet));
		try {
			
			this.messagetype = dis.readInt();
			this.id = dis.readLong();
			this.ip = dis.readUTF();
			this.port = dis.readInt();
			MessageType op = MessageType.getType(this.messagetype);
			switch(op) {
			case PUT:
				//PUT Operation
				this.key = dis.readLong();
				this.version = dis.readLong();
				int valuesize = dis.readInt();
				this.value = new byte[valuesize];
				dis.read(value, 0, valuesize);
				break;
			case GET:
				//GET Operation
				this.key = dis.readLong();
				this.version = dis.readLong();
				this.reqid = dis.readUTF();
				break;
			case EXCHANGE:
				//Exchange Operation
				int keynumber = dis.readInt();
				this.keys = new HashSet<StoreKey>();
				while(keynumber>0){
					StoreKey tmp = new StoreKey(dis.readLong(),dis.readLong());
					this.keys.add(tmp);
					keynumber=keynumber -1;
				}
				int hashnumber = dis.readInt();
				while(hashnumber>0){
					StoreKey tmp = new StoreKey(dis.readLong(),dis.readLong());
					int filehashes = dis.readInt();
					ArrayList<String> hsls = new ArrayList<String>();
					while(filehashes>0){
						hsls.add(dis.readUTF());
					}
					this.hashlist.put(tmp, hsls);
				}
				break;
			case GETREPLY:
				//GET reply
				this.key = dis.readLong();
				this.version = dis.readLong();
				this.reqid = dis.readUTF();
				int nvaluesize = dis.readInt();
				this.value = new byte[nvaluesize];
				dis.read(value, 0, nvaluesize);

				break;
			case PUTREPLY:
				//PUT reply
				this.key = dis.readLong();
				this.version = dis.readLong();
				break;
			default: 
				break;
			}
			dis.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public byte[] encodeMessage(){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		byte[] res = null;
		try {
			MessageType op = MessageType.getType(this.messagetype);
			switch(op) {
			case PUT:
				//PUT Operation
				out.writeInt(this.messagetype);
				out.writeLong(id);
				out.writeUTF(ip);
				out.writeInt(port);
				out.writeLong(key);
				out.writeLong(version);

				out.writeInt(value.length);
				out.write(value);

				out.flush();
				res = baos.toByteArray();
				out.close();
				break;
			case GET:
				//GET Operation
				out.writeInt(this.messagetype);
				out.writeLong(id);
				out.writeUTF(ip);
				out.writeInt(port);
				out.writeLong(key);
				out.writeLong(version);
				
				out.writeUTF(reqid);

				out.flush();
				res = baos.toByteArray();
				out.close();
				break;
			case EXCHANGE:
				//Exchange message
				out.writeInt(this.messagetype);
				out.writeLong(id);
				out.writeUTF(ip);
				out.writeInt(port);
				out.writeInt(keys.size());
				for(StoreKey k : keys){
					out.writeLong(k.key);
					out.writeLong(k.version);
				}
				out.flush();
				res = baos.toByteArray();
				out.writeInt(this.hashlist.size());
				for(StoreKey k : this.hashlist.keySet()){
					out.writeLong(k.key);
					out.writeLong(k.version);
					ArrayList<String> lst = this.hashlist.get(k);
					out.writeInt(lst.size());
					for(String s : lst){
						out.writeUTF(s);
					}
				}
				out.close();
				break;
			case GETREPLY:
				//GET reply
				out.writeInt(this.messagetype);
				out.writeLong(id);
				out.writeUTF(ip);
				out.writeInt(port);
				out.writeLong(key);
				out.writeLong(version);
				out.writeUTF(reqid);
				out.writeInt(value.length);
				out.write(value);
				
				out.flush();
				res = baos.toByteArray();
				out.close();
				break;
			case PUTREPLY:
				//PUT reply
				out.writeInt(this.messagetype);
				out.writeLong(id);
				out.writeUTF(ip);
				out.writeInt(port);
				out.writeLong(key);
				out.writeLong(version);
				
				out.flush();
				res = baos.toByteArray();
				out.close();
				break;
			default: 
				//4 snapshot
				//5 stop
				out.writeInt(this.messagetype);
				out.writeLong(id);
				out.writeUTF(ip);
				out.writeInt(port);
				
				out.flush();
				res = baos.toByteArray();
				out.close();
				break;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}
	
	public static byte[] encodeMessageGet(String ip,int port,long key,long version,String reqid,long id){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		byte[] res = null;
		try {
			out.writeInt(MessageType.getValueType(MessageType.GET));
			out.writeLong(id);
			out.writeUTF(ip);
			out.writeInt(port);
			out.writeLong(key);
			out.writeLong(version);
			
			out.writeUTF(reqid);
			
			out.flush();
			res = baos.toByteArray();
			out.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return res;
	}
	
	public static byte[] encodeMessagePut(String ip,int port,long key,long version,byte[] value, long id){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		byte[] res = null;
		try {
			out.writeInt(MessageType.getValueType(MessageType.PUT));
			out.writeLong(id);
			out.writeUTF(ip);
			out.writeInt(port);
			out.writeLong(key);
			out.writeLong(version);
			
			out.writeInt(value.length);
			out.write(value);
			
			out.flush();
			res = baos.toByteArray();
			out.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}
	
	@Override
	public String toString(){
		String res = "Message of type:";
		res = res+messagetype+" ip:"+ip+" port:"+port+" key:"+key+" version: "+version+" value:"+value+" requestID:"+reqid+"\n";
		return res;
	}
}
