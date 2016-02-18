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
package pt.haslab.dataflasks.pss;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import pt.haslab.dataflasks.common.DFLogger;

import pt.haslab.dataflasks.pss.PSSMessage.TYPE;
import pt.haslab.dataflasks.common.PeerData;
import pt.haslab.dataflasks.core.GroupConstruction;
import pt.haslab.dataflasks.core.Peer;


public class PSS extends Thread{
	
	private HashMap<String,PeerData> myview;
	private HashMap<Integer,ArrayList<PeerData>> peersForSlice;
	private ArrayList<PeerData> sentPeerData;
	
	public DFLogger log;
	private Random grandom;
	private String ip;
	private int port;
	private long id;
	
	private int viewsize;
	private int gossipsize;
	
	private long interval;
	private long boottime;
	private boolean running;
	
	private DatagramSocket sendersocket;
	
	private GroupConstruction groupc;
	
	
	public PSS(String toContact, String ip,long id,long sleep,long boottime, int viewsize,DFLogger log, GroupConstruction groupC){
	
	
		this.groupc = groupC;
		
		try {
			this.sendersocket = new DatagramSocket(Peer.outpssport);
		} catch (SocketException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		this.grandom = new Random();
		this.log = log;
		this.ip = ip;
		this.running = true;
		this.id = id;
	
		this.log.info("PSS init.");
	
		//LOADING CONFIG
		this.port = Peer.pssport;
		this.interval= sleep;
		this.boottime= boottime;
		this.viewsize = viewsize;
		this.gossipsize = viewsize;
	
		//CONTACT PEER TO GET A VIEW AND NPARTITIONS
		this.myview = new HashMap<String,PeerData>();
		String targetip = toContact;
		int targetport = Peer.bootport;
		boolean connected = false;
		while(!connected){
			try {
				this.log.info("Connecting to "+targetip+":"+targetport+" to retrieve initial view");
				Socket serverSocket = new Socket(targetip, targetport);
				BufferedReader in = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
				int npeers = Integer.parseInt(in.readLine());
				while(npeers > 0){
					String peerip = in.readLine();
					int peernslices = Integer.parseInt(in.readLine());
					int peerage = Integer.parseInt(in.readLine());
					int peerslice = Integer.parseInt(in.readLine());
					double peerpos = Double.parseDouble(in.readLine());
					long peermyID = Long.parseLong(in.readLine());
					this.myview.put(peerip, new PeerData(peerip,peernslices,peerage,peerslice,peerpos,peermyID));
					npeers = npeers -1;
				}
				serverSocket.close();
				connected = true;
			}
			catch(Exception e){
				this.log.error("ERROR conecting to socket. Retrying...");
				try {
					Thread.sleep(this.grandom.nextInt(5000));
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	
		this.log.info("PSS initializing... received view:"+this.myview.keySet().toString());
	
		this.peersForSlice = new HashMap<Integer,ArrayList<PeerData>>();
		this.sentPeerData = new ArrayList<PeerData>();
	
		this.log.info("PSS initialized.");
	
		
	}

	public PSS(String ip,long id,long sleep,long boottime, int viewsize, GroupConstruction groupC){
		
		this.groupc = groupC;
		
		try {
			this.sendersocket = new DatagramSocket(Peer.outpssport);
		} catch (SocketException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		this.grandom = new Random();
		this.ip = ip;
		this.running = true;
		this.id = id;
	
		//LOADING CONFIG
		this.port = Peer.pssport;
		this.interval= sleep;
		this.boottime= boottime;
		this.viewsize = viewsize;
		this.gossipsize = viewsize;
		
		this.peersForSlice = new HashMap<Integer,ArrayList<PeerData>>();
	}
	
	public void readFromStringList(String[] lines){
		int svs = Integer.parseInt(lines[0]);
		this.myview = new HashMap<String,PeerData>();
		int i = 1;
		for(int j=0;j<svs;j++){
			String key = lines[i];
			PeerData v = new PeerData(lines[i+1]);
			this.myview.put(key,v);
			i = i + 2;
		}
		int sentsvs = Integer.parseInt(lines[i]);
		this.sentPeerData = new ArrayList<PeerData>();
		i = i +1;
		for(int j=0;j<sentsvs;j++){
			this.sentPeerData.add(new PeerData(lines[i]));
			i = i + 1;
		}
	}
	
	//USE outside SIMULATION ONLY
	
	@Override
	public String toString(){
		String res = this.myview.size() + "\n";
		for(String ps : this.myview.keySet()){
			res = res + ps + "\n" + this.myview.get(ps).toString() + "\n";
		}
		//Peers For slice is not being written.
		res = res + this.sentPeerData.size() + "\n";
		for(PeerData pd : this.sentPeerData){
			res = res + pd.toString() + "\n";
		}
		return res;
	}

	
	public void stopPSS(){
		this.groupc.stop();
		//this.sendersocket.close();
		this.running = false;
		//this.log.info("PSS stopped.");
	}
	
	//----------------------------
	
	

	@Override
	public void run() {
		long time = 0;
		try {
			Thread.sleep(boottime);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		int cycles = 0;
		while(running){
			
				try {
					Thread.sleep(this.interval);
					if(running){ //Treat the case where the peer was stopped but the thread was assleep;
						time = time + this.interval;
						this.log.info("PSS TIME: "+time);
						cycles = cycles + 1;
						this.groupc.setCycle(cycles);
						synchronized(this){

							//CYCLON
							this.insertSentToView();
							this.ageGlobal();
							PeerData target = this.getOlderGlobal();
							this.myview.remove(target.getIp());
							ArrayList<PeerData> toglobal = this.selectToSendGlobal(target.getIp());
							this.sentPeerData = new ArrayList<PeerData>();
							for (PeerData p : toglobal){
								this.sentPeerData.add((PeerData)p.clone());
							}
							this.sentPeerData.remove(this.ip);
							PSSMessage msgglobal = new PSSMessage(toglobal,TYPE.GLOBAL,this.ip);
							this.sendMsg(target, msgglobal);
							log.info("PSS message sent to "+target);

						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
	}


	
	public synchronized void processMessage(PSSMessage pmsg){
		//Incorporate peers in our state
		log.debug("PSS received message to process.");
		if(pmsg==null){
			log.error("PSS processMessage MESSAGE is NULL!");
			return;
		}
		try{
			if(pmsg.type==TYPE.LOCAL){
				this.groupc.receiveLocalMessage(pmsg.list);
				log.info("PSS local message recieved.");
			}
			else{
				if(pmsg.type==TYPE.LOADBALANCE){
					log.info("Load Balancer Request received.");
					ArrayList<PeerData> currentview = this.getView();
					PSSMessage torespond = new PSSMessage(currentview,PSSMessage.TYPE.LOADBALANCE,this.ip);
					this.sendLBMsg(pmsg.sender, torespond);
				}
				else{
					this.incorporateToGlobal(pmsg);
					this.groupc.receiveMessage(pmsg.list);;
					log.info("PSS global message received.");
				}
			}
			log.info("PSS message processed.");
		}
		catch(Exception e){
			log.error("processMessage ERROR");
		}
		
	}


	private synchronized int sendMsg(PeerData p, PSSMessage psg){
		try {
			
			byte[] toSend = psg.encodeMessage();
			this.log.debug("pss message size = "+toSend.length);
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()),  this.port);
			this.log.debug("sending message to "+p.getIp()+":"+ this.port);
			sendersocket.send(packet);	
			this.log.debug("message sent to "+p.getIp()+":"+ this.port+" Message:"+ packet.toString());
			return 0;
		} catch (IOException e) {
			this.log.error("ERROR sendget in PEER. "+e.getMessage()+" IP:PORT" + p.getIp()+":"+ this.port);
			//e.printStackTrace();
		}
		return 1;
	}
	
	private synchronized int sendLBMsg(String target, PSSMessage psg){
		try {
			
			byte[] toSend = psg.encodeMessage();
			this.log.debug("pss message size = "+toSend.length);
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(target),Peer.lbport);
			this.log.debug("sending message to "+target+":"+Peer.lbport);
			sendersocket.send(packet);	
			this.log.debug("message sent to "+target+":"+Peer.lbport+" Message:"+ packet.toString());
			return 0;
		} catch (IOException e) {
			this.log.error("ERROR sendget in PEER. "+e.getMessage()+" IP:PORT" + target+":"+Peer.lbport);
			//e.printStackTrace();
		}
		return 1;
	}
	
	
	private synchronized void insertSentToView(){
		while(this.myview.size()<this.viewsize && this.sentPeerData.size()>0){
			PeerData tmp = this.sentPeerData.remove(0);
			this.myview.put(tmp.getIp(), tmp);
		}
	}
	
	private synchronized void insertSentToView(ArrayList<PeerData> source){
		while(this.myview.size()<this.viewsize && source.size()>0){
			PeerData tmp = source.remove(0);
			if(tmp.getID()!=this.id){
				PeerData current = (PeerData)this.myview.get(tmp.getIp());
				if(current!=null){
					int currentage =current.getAge();
					if(currentage>tmp.getAge()){
						this.myview.put(tmp.getIp(), tmp);
					}
				}
				else{
					this.myview.put(tmp.getIp(), tmp);
				}
			}
		}
	}
	
	private synchronized ArrayList<PeerData> selectToSendGlobal(String p){
		PeerData myinfo = new PeerData(this.ip, this.groupc.getNgroups(),0,this.groupc.getGroup(),this.groupc.getPosition(),this.id);
		ArrayList<PeerData> res = new ArrayList<PeerData>();
		res.add(myinfo);
		Collection<PeerData> values = this.myview.values();
		ArrayList<PeerData> tmp = new ArrayList<PeerData>();
		for(PeerData tp : values){
			if(!tp.getIp().equals(p)){
				tmp.add((PeerData)tp.clone());
			}
		}
		Collections.shuffle(tmp,this.grandom);
		for (PeerData pe : tmp){
			if(res.size()<this.gossipsize){
				res.add(pe);
				this.myview.remove(pe.getIp());
			}
		}
		return res;
	}

	private synchronized void incorporateToGlobal(PSSMessage msg){
		log.debug("message received from "+msg.sender);
		ArrayList<PeerData> source = new ArrayList<PeerData>();
		for(PeerData ptmp : msg.list){
			if(ptmp.getID()!=this.id){
				source.add((PeerData)ptmp.clone());
			}
		}
		if(msg.type==TYPE.RESPONSE){
			log.debug("answer received from "+msg.sender);
			this.insertSentToView(source);
			this.insertSentToView();
			while(this.myview.size()>this.viewsize){
				this.removeOlderGlobal();
			}
			this.sentPeerData = new ArrayList<PeerData>();
		}
		else{
			if(msg.type==TYPE.GLOBAL){
				log.debug("message received from "+msg.sender);
				ArrayList<PeerData> tosend = this.selectToSendGlobal(msg.sender);
				ArrayList<PeerData> tofillemptyview = new ArrayList<PeerData>();
				for(PeerData p : tosend){
					if(p.getID()!=this.id){
						tofillemptyview.add((PeerData)p.clone());
					}
				}
				PeerData target = null;
				for(PeerData rec : msg.list){
					if(rec.getIp().equals(msg.sender)){
						target = (PeerData)rec.clone();
					}
				}
				this.insertSentToView(source);
				this.insertSentToView(tofillemptyview);
				while(this.myview.size()>this.viewsize){
					this.removeOlderGlobal();
				}
				PSSMessage msgglobal = new PSSMessage(tosend,TYPE.RESPONSE,this.ip);
				this.sendMsg(target, msgglobal);
				log.info("PSS answer message sent to "+msg.sender);
			}
		}
	}


	private synchronized void removeOlderGlobal(){
		PeerData toremove = null;
		for (PeerData pe : this.myview.values()){
			if(toremove == null){
				toremove = pe;
			}
			else{
				if(pe.getAge()>toremove.getAge()){
					toremove = pe;
				}
			}
		}
		this.myview.remove(toremove.getIp());
		this.log.debug("removed older: "+toremove.getIp());
	}

	
	private synchronized PeerData getOlderGlobal(){
		PeerData older = null;
		for (PeerData pe : this.myview.values()){
			if(older == null){
				older = pe;
			}
			else{
				if(pe.getAge()>older.getAge()){
					older = pe;
				}
			}
		}
		return (PeerData)older.clone();
	}
	
	private synchronized void ageGlobal(){
		for (PeerData pe : this.myview.values()){
			pe.age();
		}
	}

	
	public synchronized ArrayList<PeerData> getView(){
		ArrayList<PeerData> res = new ArrayList<PeerData>();
		ArrayList<PeerData> tmp = new ArrayList<PeerData>();
		tmp.addAll(this.myview.values());
		for(PeerData tp : tmp){
			res.add((PeerData)tp.clone());
		}
		for(PeerData tp2 : this.sentPeerData){
			res.add((PeerData)tp2.clone());
		}
		return res;
	}
	
	public synchronized PeerData getRandomPeerFromLocalView(){
		ArrayList<PeerData> localv = this.groupc.getLocalView();
		if(localv.isEmpty()){
			return null;
		}
		else{
			Collections.shuffle(localv);
			return localv.get(0);
		}
	}
	
	public synchronized ArrayList<PeerData> getSliceLocalView(int slice){
		if(slice==this.groupc.getGroup()){
			return this.getSliceLocalView();
		}
		else{
			ArrayList<PeerData> tmp = this.peersForSlice.get(slice);
			ArrayList<PeerData> res = new ArrayList<PeerData>();
			for(PeerData tp : tmp){
				res.add((PeerData)tp.clone());
			}
			return res;
		}
	}
	
	public synchronized ArrayList<PeerData> getSliceLocalView(){
		return this.groupc.getLocalView();
	}


	public synchronized ArrayList<PeerData> havePeerFromSlice(int slice) {
		return new ArrayList<PeerData>();
	}
	
	
	
}


