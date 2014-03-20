package handler;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import org.apache.log4j.Logger;

import pss.PSS;
import store.KVStore;
import common.PeerData;
import core.Peer;

public class AntiEntropy implements Runnable{

	private PSS view;
	private KVStore store;
	private long interval;
	private boolean running;
	private Random grandom;
	private Logger log;
	private String myIp;
	private int myPort;
	private long myID;
	
	public void setRunning(boolean nb){
		this.running = nb;
	}
	
	public AntiEntropy(String myIp,int myPort,long myID,PSS v,KVStore store,long interval,Random r,Logger log){
		this.running = true;
		this.view =v;
		this.grandom = r;
		this.log = log;
		this.interval = interval;
		this.store = store;
		this.myIp = myIp;
		this.myPort = myPort;
		this.myID = myID;
	}
	 
	
	
	
	private void sendPeerKeys(PeerData p,HashSet<Long> mykeys){
		try{
			DatagramSocket socket = new DatagramSocket();
			byte[] toSend = new Message(4,myIp,myPort,myID,mykeys).encodeMessage();
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(myIp), myPort);
			socket.send(packet);		
			socket.close();
		} catch (IOException e) {
			this.log.debug("ERROR sendPeerKeys in PEER. "+e.getMessage()+" IP:PORT" + p.getIp()+":"+Peer.port);
			//e.printStackTrace();
		}
	}
	
	
	@Override
	public void run() {
		long time = 0;
		while(running){
			
				try {
					Thread.sleep(this.interval);
					time = time + this.interval;
					ArrayList<PeerData> myneighbors = view.getSliceLocalView();
					int localsize = myneighbors.size();
					if(localsize > 0){
						PeerData toContact = myneighbors.get(grandom.nextInt(localsize));
						log.debug(time+" to contact:"+toContact.getID());

						//Contact Peer in order to check if there are missing objects
						HashSet<Long> mykeys = this.store.getKeys();
						this.sendPeerKeys(toContact,mykeys);
					}
					
					
				} catch (InterruptedException e) {
					log.error("ERROR in ACTIVETHREAD!");
					e.printStackTrace();
				}
				
			
		}
		
	}

	

}
