package pt.haslab.dataflasks.messaging;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class PutReplyMessage implements MessageInterface {
	
	public final MessageType messagetype = MessageType.PUTREPLY;
	public String ip;
	public int port;
	public long key;
	public long version;
	public long id;
	
	public PutReplyMessage(){
		
	}
	
	public PutReplyMessage(String ip,int port,long key,long version,long id){
		this.ip = ip;
		this.port = port;
		this.key = key;
		this.version = version;
		this.id = id;
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
		return this.key;
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
