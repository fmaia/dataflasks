package pss;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.apache.log4j.Logger;

import pss.PSSMessage.TYPE;
import common.PeerData;
import core.GroupConstruction;
import core.Peer;


public class PSS extends Thread implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private HashMap<String,PeerData> myview;
	private HashMap<Integer,ArrayList<PeerData>> peersForSlice;
	private ArrayList<PeerData> sentPeerData;
	
	private Logger log;
	private Random grandom;
	private String ip;
	private int port;
	private long id;
	
	private int viewsize;
	private int gossipsize;
	
	private long interval;
	private long boottime;
	private boolean running;
	
	private GroupConstruction groupc;
	
	
	public PSS(String toContact, String ip,long id,long sleep,long boottime, int viewsize,Logger log, GroupConstruction groupC){
	
	
		this.groupc = groupC;
		
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
				log.error("ERROR conecting to socket. Retrying...");
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


	public void writeObject(ObjectOutputStream s){
		try {
			s.writeObject(this.myview);
			s.writeObject(this.peersForSlice);
			s.writeObject(this.sentPeerData);
		} catch (IOException e) {
			log.error("Error serializing object Store.");
			e.printStackTrace();
		}
	}
	
	
	@SuppressWarnings("unchecked")
	public void readObject(ObjectInputStream s){
		try {
			this.myview = (HashMap<String,PeerData>)s.readObject();
			this.peersForSlice =(HashMap<Integer,ArrayList<PeerData>>) s.readObject();
			this.sentPeerData = (ArrayList<PeerData>)s.readObject();
		} catch (IOException e) {
			log.error("Error reading object Store from file.");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			log.error("Error reading object Store from file.");
			e.printStackTrace();
		}
	}
	
	
	//USE outside SIMULATION ONLY
	
	public void stopPSS(){
		this.running = false;
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
					time = time + this.interval;
					this.log.info("TIME: "+time);
					cycles = cycles + 1;
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
						log.debug("message sent to "+target);

					}
					
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
	}


	
	public synchronized void processMessage(PSSMessage pmsg){
		//Incorporate peers in our state
		this.log.debug("PSS received message to process.");
		if(pmsg==null){
			this.log.error("PSS processMessage MESSAGE is NULL!");
			return;
		}
		try{
			if(pmsg.type==TYPE.LOCAL){
				this.groupc.receiveLocalMessage(pmsg.list);
			}
			else{
				this.incorporateToGlobal(pmsg);
				this.groupc.receiveLocalMessage(pmsg.list);
			}
			this.log.debug("Message processed by PSS.");
		}
		catch(Exception e){
			log.error("processMessage ERROR");
		}
		
	}


	private synchronized int sendMsg(PeerData p, PSSMessage psg){
		try {
			DatagramSocket socket = new DatagramSocket();
			byte[] toSend = psg.encodeMessage();
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()), this.port);
			this.log.debug("sending message to "+p.getIp()+":"+this.port);
			socket.send(packet);	
			this.log.info("message sent to "+p.getIp()+":"+this.port+" Message:"+ packet.toString());
			socket.close();
			return 0;
		} catch (IOException e) {
			this.log.error("ERROR sendget in PEER. "+e.getMessage()+" IP:PORT" + p.getIp()+":"+this.port);
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
			if(tmp.getIp()!=this.ip){
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
		log.info("message received from "+msg.sender);
		ArrayList<PeerData> source = new ArrayList<PeerData>();
		for(PeerData ptmp : msg.list){
			if(ptmp.getIp()!=this.ip){
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
					if(p.getIp()!=this.ip){
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
				log.debug("answer sent to "+msg.sender);
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
	
	public synchronized PeerData getRandomPeer(){
		PeerData res = null;
		int s =  this.myview.values().size() + this.sentPeerData.size();
		int pos = this.grandom.nextInt(s);
		ArrayList<PeerData> tmp = new ArrayList<PeerData>();
		tmp.addAll(this.myview.values());
		tmp.addAll(this.sentPeerData);
		Iterator<PeerData> it = tmp.iterator();
		while(pos>0 && it.hasNext()){
			res = (PeerData)it.next();
			pos = pos - 1;
		}
		return res;
	}
	
	public synchronized ArrayList<PeerData> getSliceLocalView(int slice){
		ArrayList<PeerData> tmp = this.peersForSlice.get(slice);
		ArrayList<PeerData> res = new ArrayList<PeerData>();
		for(PeerData tp : tmp){
			res.add((PeerData)tp.clone());
		}
		return res;
	}
	
	public synchronized ArrayList<PeerData> getSliceLocalView(){
		ArrayList<PeerData> res = new ArrayList<PeerData>();
		
		return res;
	}


	public synchronized ArrayList<PeerData> havePeerFromSlice(int slice) {
		return new ArrayList<PeerData>();
	}
	
	
	
}


