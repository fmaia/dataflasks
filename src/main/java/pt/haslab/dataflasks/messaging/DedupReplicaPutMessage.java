package pt.haslab.dataflasks.messaging;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;

import pt.haslab.dataflasks.store.StoreKey;

public class DedupReplicaPutMessage implements MessageInterface {

	public MessageType messagetype = MessageType.DEDUPPUT;
	public String ip;
	public int port;
	public long id;
	public long key;
	public long version;
	public HashMap<String,byte[]> blocks;
	
	public DedupReplicaPutMessage(){
		
	}
	
	public DedupReplicaPutMessage(String ip, int port,long id, HashMap<String,byte[]> blocks, StoreKey key){
		this.ip = ip;
		this.port = port;
		this.id = id;
		this.blocks = blocks;
		this.key = key.key;
		this.version = key.version;
	}
	
	public void decodeMessage(byte[] packet) {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(packet));
		try {
			dis.readInt(); //ignoring messagetype
			this.id = dis.readLong();
			this.ip = dis.readUTF();
			this.port = dis.readInt();
			this.key = dis.readLong();
			this.version = dis.readLong();
			int keynumber = dis.readInt();
			this.blocks = new HashMap<String,byte[]>();
			while(keynumber>0){
				String tmp = dis.readUTF();
				int nvaluesize = dis.readInt();
				byte[] block = new byte[nvaluesize];
				dis.read(block, 0, nvaluesize);
				this.blocks.put(tmp, block);
				keynumber=keynumber -1;
			}
			
			dis.close();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
			out.writeLong(key);
			out.writeLong(version);
			out.writeInt(blocks.size());
			for(String k : blocks.keySet()){
				out.writeUTF(k);
				byte[] block = blocks.get(k);
				out.writeInt(block.length);
				out.write(block);
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
