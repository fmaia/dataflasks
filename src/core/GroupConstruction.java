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
package core;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;

import common.DFLogger;

import pss.PSSMessage;
import pss.PSSMessage.TYPE;
import store.KVStore;
import common.PeerData;



public class GroupConstruction {

	
	private ArrayList<PeerData> localview;
	private long id;
	private String ip;
	public double position;
	private int ngroups;
	private int group;
	private int replicationfactorMax;
	private int replicationfactorMin;
	private int maxage;
	private boolean local;
	private int localinterval;
	private KVStore store;
	public DFLogger log;
	
	private DatagramSocket sendersocket;
	
	private int cycle;
	
	private boolean firstmessage;
	
	
	public GroupConstruction(KVStore thestore){
		this.store = thestore;
		this.firstmessage = true;
		try {
			this.sendersocket = new DatagramSocket(Peer.outgroupport);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//USE OUTSIDE SIMULATION------------------------------------------------------------
	
	
	@Override
	public String toString(){
		String res = this.ngroups + "\n" + this.group + "\n";
		res = res + this.ip + "\n" + this.id + "\n" + this.position + "\n";
		res = res + this.replicationfactorMin + "\n" + this.replicationfactorMax +"\n";
		res = res + this.maxage + "\n" + this.local + "\n" + this.localinterval + "\n";
		res = res + this.localview.size() + "\n";
		for (PeerData p : this.localview){
			res = res + p.toString() + "\n";
		}
		return res;
	}
	
	public void readFromString(String[] s){
		String[] lines = s;
		this.ngroups = Integer.parseInt(lines[0]);
		this.group = Integer.parseInt(lines[1]);
		this.ip = lines[2];
		this.id = Long.parseLong(lines[3]);
		this.position = Double.parseDouble(lines[4]);
		this.replicationfactorMin = Integer.parseInt(lines[5]);
		this.replicationfactorMax = Integer.parseInt(lines[6]);
		this.maxage = Integer.parseInt(lines[7]);
		this.local = Boolean.parseBoolean(lines[8]);
		this.localinterval = Integer.parseInt(lines[9]);
		int sizel = Integer.parseInt(lines[10]);
		this.localview = new ArrayList<PeerData>();
		for(int i=11;i<sizel+11;i++){
			PeerData p = new PeerData(lines[i]);
			this.localview.add(p);
		}
	}
	
	
	//----------------------------------------------------------------------------------
	
	public GroupConstruction(String ip,long id,double position, int replicationfactorMin,
			int replicationfactorMax, int maxage, boolean local,int localinterval,KVStore thestore, DFLogger log){
		this.firstmessage = false;
		this.id = id;
		this.position = position;
		this.ngroups = 1;
		this.group = 1;
		this.replicationfactorMin = replicationfactorMin;
		this.replicationfactorMax = replicationfactorMax;
		this.localview = new ArrayList<PeerData>();
		this.maxage = maxage;
		this.local = local;
		this.localinterval = localinterval;
		this.store = thestore;
		this.log = log;
		this.log.info("GroupConstruction initialized.");
		this.cycle = 1;
		this.ip = ip;
		try {
			this.sendersocket = new DatagramSocket(Peer.outgroupport);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void stop(){
		//this.sendersocket.close();
	}
	
	//Use outside simulation
	public String getInfo(){
		String res = "INFO "+this.id+" "+this.group+" "+this.ngroups;
		return res;
	}
	//----------------------
	
	public synchronized ArrayList<PeerData> getLocalView(){
		ArrayList<PeerData> res = new ArrayList<PeerData>();
		for (PeerData p : this.localview){
			res.add((PeerData)p.clone());
		}
		return res;
	}
	
	public synchronized int getCycle() {
		return cycle;
	}

	public synchronized void setCycle(int cycle) {
		this.cycle = cycle;
	}

	public double getPosition(){
		return this.position;
	}
	

	public synchronized int getNgroups() {
		return ngroups;
	}


	public synchronized void setNgroups(int ngroups) {
		this.ngroups = ngroups;

	}


	public synchronized int getGroup() {
		return this.group;
	}


	public synchronized void setGroup(int group) {
		this.group = group;
	}

	private int group(double peerpos){
		int temp = (int) Math.ceil((new Double(this.ngroups))*peerpos);
		if(temp == 0){
			temp = 1;
		}
		return temp;
	}
	

	public synchronized void receiveLocalMessage(ArrayList<PeerData> received) {
		this.log.debug("GroupConstruction - LOCAL message received");
		//ADD RECEIVED
		for (PeerData r : received){
			if(group(r.getPos())==this.group && !(r.getID()==this.id)){
				int index = this.localview.indexOf(r);
				if(index==-1){
					this.localview.add(r);
				}
				else{
					PeerData current = this.localview.get(index);
					if(current.getAge()>r.getAge()){
						current.setAge(r.getAge());
					}
				}
			}
		}

	}


	public synchronized void receiveMessage(ArrayList<PeerData> received) {
		this.log.debug("GroupConstruction - PSS message received");
		
		//When a peer boots it must keep up with peers
		int ngroupsfrompeers = 1;
		
		//AGING VIEW
		for(PeerData r : localview){
			r.setAge(r.getAge() + 1);
		}

		//ADD RECEIVED
		for (PeerData r : received){
			if(this.firstmessage){
				if(r.getNslices()>this.ngroups){
					ngroupsfrompeers = r.getNslices();
				}
			}
			if(group(r.getPos())==this.group && !(r.getID()==this.id)){
				if(!this.localview.contains(r)){
					this.localview.add(r);
				}
				else{
					int index = this.localview.indexOf(r);
					if(this.localview.get(index).getAge()>r.getAge()){
						this.localview.remove(index);
						this.localview.add(r);
					}
				}
			}
		}

		//CLEAN VIEW
		ArrayList<PeerData> torem = new ArrayList<PeerData>();
		for (PeerData r : this.localview){
			if(group(r.getPos())!=this.group){
				torem.add(r);
			}
			else{
				if(r.getAge()>maxage){
					torem.add(r);
				}
			}
		}
		for(PeerData r : torem){
			this.localview.remove(r);
		}

		//SEARCH FOR VIOLATIONS
		int estimation = this.localview.size(); //countEqual();
		this.log.debug("GroupConstruction - estimation = "+estimation);
		if((estimation+1)<this.replicationfactorMin){
			if(this.ngroups>1){
				this.ngroups = this.ngroups/2; 
			}
		}
		if((estimation+1)>this.replicationfactorMax){
			this.ngroups = this.ngroups*2;
		}
		if(this.firstmessage){
			this.ngroups = ngroupsfrompeers;
			this.firstmessage = false;
		}
		this.group = group(this.position);
		this.store.updatePartition(this.group, this.ngroups);


		//SEND LOCAL VIEW TO NEIGHBORS
		if(this.local && (this.cycle%this.localinterval==0)){
			ArrayList<PeerData> tosend = new ArrayList<PeerData>();
			PeerData myself = new PeerData(this.ip,this.ngroups,0,this.group,this.position,this.id);
			tosend.add(myself);
			for(PeerData r : this.localview){
				tosend.add((PeerData)r.clone());
			}
			this.log.debug("Local Group Dissemination Active. Cycle: "+cycle+" TOSENDsize:"+tosend.size());
			this.log.info("Group Dissemination - sending local view. Cycle: "+cycle);
			for(PeerData r : tosend){
				this.log.debug("GroupConstruction - tosend ids: "+r.getID());
				if(r.getID()!=this.id){
					//SEND MESSAGE
					this.log.debug("Group Construction Sending local message...");
					PSSMessage tsmsg = new PSSMessage(tosend, TYPE.LOCAL, this.ip);
					this.log.debug("Group Construction created message to send.");
					int res = this.sendMsg(r, tsmsg);
					this.log.debug("Group Construction local message sent to "+r.getID()+" res="+res);
				}
			}
		}

	}
	
	private synchronized int sendMsg(PeerData p, PSSMessage psg){
		try {
			this.log.debug("GroupConstruction - going to send message to "+p.getIp()+":"+Peer.pssport);
			
			byte[] toSend = psg.encodeMessage();
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()), Peer.pssport);
			this.log.debug("GroupConstruction - sending message to "+p.getIp()+":"+Peer.pssport);
			this.sendersocket.send(packet);	
			this.log.debug("GroupConstruction - message sent to "+p.getIp()+":"+Peer.pssport+" Message:"+ packet.toString());
			return 0;
		} catch (IOException e) {
			this.log.error("ERROR sendget in GROUPCONSTRUCTION. "+e.getMessage()+" IP:PORT" + p.getIp()+":"+Peer.pssport);
			//e.printStackTrace();
		}
		return 1;
	}

}
