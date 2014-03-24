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
package pss;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import common.PeerData;

public class PSSMessage {

	public ArrayList<PeerData> list;
	public int type;
	public String sender;
	
	public class TYPE {
		public static final int GLOBAL = 0;
		public static final int LOCAL = 1;
		public static final int RESPONSE = 2;
		
	}
	
	
	public PSSMessage(int t, String sender){
		this.type = t;
		this.list = new ArrayList<PeerData>();
		this.sender = sender;
	}
	
	public PSSMessage(ArrayList<PeerData> p,int t, String sender){
		this.type = t;
		this.sender = sender;
		this.list = new ArrayList<PeerData>();
		for(PeerData tp : p){
			this.list.add((PeerData)tp.clone());
		}
	}
	
	public PSSMessage(byte[] packet, Logger log){
		log.debug("PSSMessage constructor. Going to read DataPacket.");
		try{
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(packet));
		this.list = new ArrayList<PeerData>();

		this.type = dis.readInt();
		this.sender = dis.readUTF();
		int listSize = dis.readInt();

		for(int i=0;i<listSize;i++){
			String tip = dis.readUTF();
			int tnslices = dis.readInt();
			int tage = dis.readInt();
			int tslice = dis.readInt();
			double tpos = dis.readDouble();
			long tid = dis.readLong();
			PeerData np = new PeerData(tip,tnslices,tage,tslice,tpos,tid);
			this.list.add(np);
		}
		log.debug("PSSMessage read with size: "+this.list.size());
		dis.close();
		}
		catch(Exception e){
			log.error("ERROR in PSSMessage Constructor.");
		}

	}
	
	public void addPeerToMessage(PeerData p){
		this.list.add(p);
	}
	
	public byte[] encodeMessage(){
		byte[] res = null;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(baos);
			out.writeInt(this.type);
			out.writeUTF(this.sender);
			out.writeInt(this.list.size());
			for(PeerData p : this.list){
				out.writeUTF(p.getIp());
				out.writeInt(p.getNslices());
				out.writeInt(p.getAge());
				out.writeInt(p.getSlice());
				out.writeDouble(p.getPos());
				out.writeLong(p.getID());
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
	
}
