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
import java.util.ArrayList;

import org.apache.log4j.Logger;

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
	private Logger log;
	
	private int cycle;
	
	public GroupConstruction(String ip,long id,double position, int replicationfactorMin,
			int replicationfactorMax, int maxage, boolean local,int localinterval,KVStore thestore, Logger log){
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

			//AGING VIEW
			for(PeerData r : localview){
				r.setAge(r.getAge() + 1);
			}

			//ADD RECEIVED
			for (PeerData r : received){
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
			DatagramSocket socket = new DatagramSocket();
			byte[] toSend = psg.encodeMessage();
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()), Peer.pssport);
			this.log.debug("GroupConstruction - sending message to "+p.getIp()+":"+Peer.pssport);
			socket.send(packet);	
			this.log.debug("GroupConstruction - message sent to "+p.getIp()+":"+Peer.pssport+" Message:"+ packet.toString());
			socket.close();
			return 0;
		} catch (IOException e) {
			this.log.error("ERROR sendget in GROUPCONSTRUCTION. "+e.getMessage()+" IP:PORT" + p.getIp()+":"+Peer.pssport);
			//e.printStackTrace();
		}
		return 1;
	}

}
