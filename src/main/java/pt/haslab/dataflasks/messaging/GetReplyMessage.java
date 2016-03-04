package pt.haslab.dataflasks.messaging;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class GetReplyMessage implements MessageInterface {

	public final MessageType messagetype = MessageType.GETREPLY;
	public String ip;
	public int port;
	public long key;
	public long version;
	public String reqid;
	public long id;
	public byte[] value;
	
	
	public GetReplyMessage(){
		
	}
	
	public GetReplyMessage(String ip, int port,byte[] value,String reqid,Long key,Long version, long id){
		this.ip = ip;
		this.port = port;
		this.key = key;
		this.version = version;
		this.reqid = reqid;
		this.id = id;
		this.value = value;
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
			this.reqid = dis.readUTF();
			int nvaluesize = dis.readInt();
			this.value = new byte[nvaluesize];
			dis.read(value, 0, nvaluesize);
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
			out.writeInt(MessageType.getValueType(MessageType.GETREPLY));
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
