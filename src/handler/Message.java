package handler;

import java.io.*;
import java.util.HashSet;


public class Message {

	public int messagetype;
	public String ip;
	public int port;
	public long key;
	public byte[] value;
	public String reqid;
	public long id;
	public HashSet<Long> keys;
	
	public Message(int type, String ip, int port,long id){
		this.messagetype = type;
		this.id = id;
		this.ip = ip;
		this.port = port;
		this.key = 0;
		this.value = null;
		this.reqid = "-1";
	}
	
	public Message(int type, String ip, int port,long id, HashSet<Long> keys){
		this.messagetype = type;
		this.id = id;
		this.ip = ip;
		this.port = port;
		this.key = 0;
		this.value = null;
		this.reqid = "-1";
		this.keys = keys;
	}
	
	public Message(int type, String ip, int port,byte[] value,String reqid,Long key,long id){
		this.messagetype = type;
		this.id = id;
		this.ip = ip;
		this.port = port;
		this.key = key;
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
			
			switch(this.messagetype) {
			case 1:
				//SLEAD Operation
				//TODO
				break;
			case 2:
				//PUT Operation
				this.key = dis.readLong();
				int valuesize = dis.readInt();
				this.value = new byte[valuesize];
				dis.read(value, 0, valuesize);
				break;
			case 3:
				//GET Operation
				this.key = dis.readLong();
				this.reqid = dis.readUTF();
				break;
			case 4:
				//Exchange Operation
				int keynumber = dis.readInt();
				this.keys = new HashSet<Long>();
				while(keynumber>0){
					this.keys.add(dis.readLong());
					keynumber=keynumber -1;
				}
				break;
			case 10:
				//GET reply
				this.key = dis.readLong();
				this.reqid = dis.readUTF();
				int nvaluesize = dis.readInt();
				this.value = new byte[nvaluesize];
				dis.read(value, 0, nvaluesize);

				break;
			case 11:
				//PUT reply
				this.key = dis.readLong();
		
				break;
			case 23:
				//Shutdown Thread
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
			switch(this.messagetype) {
			case 1:
				//SLEAD Operation
				//TODO
				break;
			case 2:
				//PUT Operation
				out.writeInt(2);
				out.writeLong(id);
				out.writeUTF(ip);
				out.writeInt(port);
				out.writeLong(key);

				out.writeInt(value.length);
				out.write(value);

				out.flush();
				res = baos.toByteArray();
				out.close();
				break;
			case 3:
				//GET Operation
				out.writeInt(3);
				out.writeLong(id);
				out.writeUTF(ip);
				out.writeInt(port);
				out.writeLong(key);
				
				out.writeUTF(reqid);

				out.flush();
				res = baos.toByteArray();
				out.close();
				break;
			case 4:
				//Exchange message
				out.writeInt(4);
				out.writeLong(id);
				out.writeUTF(ip);
				out.writeInt(port);
				out.writeInt(keys.size());
				for(Long k : keys){
					out.writeLong(k);
				}
				out.flush();
				res = baos.toByteArray();
				out.close();
				break;
			case 10:
				//GET reply
				out.writeInt(10);
				out.writeLong(id);
				out.writeUTF(ip);
				out.writeInt(port);
				out.writeLong(key);
				out.writeUTF(reqid);
				out.writeInt(value.length);
				out.write(value);
				
				out.flush();
				res = baos.toByteArray();
				out.close();
				break;
			case 11:
				//PUT reply
				out.writeInt(11);
				out.writeLong(id);
				out.writeUTF(ip);
				out.writeInt(port);
				out.writeLong(key);
				
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
	
	public static byte[] encodeMessageGet(String ip,int port,long key,String reqid,long id){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		byte[] res = null;
		try {
			out.writeInt(3);
			out.writeLong(id);
			out.writeUTF(ip);
			out.writeInt(port);
			out.writeLong(key);
			
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
	
	public static byte[] encodeMessagePut(String ip,int port,long key,byte[] value, long id){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		byte[] res = null;
		try {
			out.writeInt(2);
			out.writeLong(id);
			out.writeUTF(ip);
			out.writeInt(port);
			out.writeLong(key);
			
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
		res = res+messagetype+" ip:"+ip+" port:"+port+" key:"+key+" value:"+value+" requestID:"+reqid+"\n";
		return res;
	}
}
