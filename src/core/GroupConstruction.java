package core;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import pss.PSSMessage;
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
	
	public GroupConstruction(long id,double position, int replicationfactorMin,
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


	public synchronized void receiveMessage(ArrayList<PeerData> received, int cycle) {
		ArrayList<PeerData> tosend = new ArrayList<PeerData>();

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

			if(local){
				PeerData myself = new PeerData(this.ip,this.ngroups,0,this.group,this.position,this.id);
				tosend.add(myself);
				for(PeerData r : this.localview){
					tosend.add((PeerData)r.clone());
				}
			}

		//SEND LOCAL VIEW TO NEIGHBORS
		if(local && (cycle%this.localinterval==0)){
			for(PeerData r : tosend){
				if(r.getID()!=this.id){
					//SEND MESSAGE
					PSSMessage tsmsg = new PSSMessage(tosend, PSSMessage.TYPE.LOCAL, this.ip);
					this.sendMsg(r, tsmsg);
				}
			}
		}

	}
	
	private synchronized int sendMsg(PeerData p, PSSMessage psg){
		try {
			DatagramSocket socket = new DatagramSocket();
			byte[] toSend = psg.encodeMessage();
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()), Peer.pssport);
			this.log.debug("sending message to "+p.getIp()+":"+Peer.pssport);
			socket.send(packet);	
			this.log.info("message sent to "+p.getIp()+":"+Peer.pssport+" Message:"+ packet.toString());
			socket.close();
			return 0;
		} catch (IOException e) {
			this.log.error("ERROR sendget in PEER. "+e.getMessage()+" IP:PORT" + p.getIp()+":"+Peer.pssport);
			//e.printStackTrace();
		}
		return 1;
	}

}
