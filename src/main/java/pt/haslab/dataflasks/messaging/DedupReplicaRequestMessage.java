package pt.haslab.dataflasks.messaging;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import pt.haslab.dataflasks.store.StoreKey;

public class DedupReplicaRequestMessage implements MessageInterface {

	
	public MessageType messagetype = MessageType.MISSINGHASHREQ;
	public String ip;
	public int port;
	public long id;
	public HashSet<StoreKey> keys;
	public HashMap<StoreKey,ArrayList<String>> hashlist;
	
	
	public DedupReplicaRequestMessage(){
	}
	
	public DedupReplicaRequestMessage(String ip, int port,long id, HashSet<StoreKey> keys, HashMap<StoreKey,ArrayList<String>> hashes){
		this.ip = ip;
		this.port = port;
		this.id = id;
		this.keys = keys;
		this.hashlist = hashes;
	}
	
	public void decodeMessage(byte[] packet) {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(packet));
		try {
			dis.readInt(); //ignoring messagetype
			this.id = dis.readLong();
			this.ip = dis.readUTF();
			this.port = dis.readInt();
			int keynumber = dis.readInt();
			this.keys = new HashSet<StoreKey>();
			while(keynumber>0){
				StoreKey tmp = new StoreKey(dis.readLong(),dis.readLong());
				this.keys.add(tmp);
				keynumber=keynumber -1;
			}
			int hashnumber = dis.readInt();
			this.hashlist = new HashMap<StoreKey,ArrayList<String>>();
			while(hashnumber>0){
				StoreKey tmp = new StoreKey(dis.readLong(),dis.readLong());
				int filehashes = dis.readInt();
				ArrayList<String> hsls = new ArrayList<String>();
				while(filehashes>0){
					hsls.add(dis.readUTF());
					filehashes = filehashes -1;
				}
				this.hashlist.put(tmp, hsls);
				hashnumber = hashnumber -1;
			}
			dis.close();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("ERROR DECODING");
			e.printStackTrace();
		}
	}

	public byte[] encodeMessage() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		byte[] res = null;
		try {
			out.writeInt(MessageType.getValueType(this.messagetype));
			out.writeLong(id);
			out.writeUTF(ip);
			out.writeInt(port);
			out.writeInt(keys.size());
			for(StoreKey k : keys){
				out.writeLong(k.key);
				out.writeLong(k.version);
			}
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
			out.flush();
			res = baos.toByteArray();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}
	
	public long getMessageKey() {
		return -1;
	}
	
	public String getMessageIP() {
		return this.ip;
	}

	public int getMessagePort() {
		return this.port;
	}
	
	public MessageType getMessageType() {
		return this.messagetype;
	}
	
	public long getMessageID() {
		return this.id;
	}
}
