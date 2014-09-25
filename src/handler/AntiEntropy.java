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
package handler;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
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
	private long boottime;
	private boolean running;
	//private Random grandom;
	private Logger log;
	private String myIp;
	private int myPort;
	private long myID;
	private DatagramSocket socketsender;
	
	public void stop(){
		//this.socketsender.close();
		this.running = false;
		this.log.info("Stopping Anti Entropy.");
	}
	
	public AntiEntropy(String myIp,int myPort,long myID,PSS v,KVStore store,long interval,long boottime,Random r,Logger log){
		this.running = true;
		this.view =v;
		//this.grandom = r;
		this.log = log;
		this.interval = interval;
		this.store = store;
		this.myIp = myIp;
		this.myPort = myPort;
		this.myID = myID;
		this.boottime = boottime;
		try {
			this.socketsender = new DatagramSocket(Peer.outantientropyport);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.log.info("Anti Entropy initialized.");
	}
	 
	
	
	
	private void sendPeerKeys(PeerData p,HashSet<Long> mykeys){
		try{
			byte[] toSend = new Message(4,myIp,myPort,myID,mykeys).encodeMessage();
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()), Peer.port);
			this.socketsender.send(packet);		
		} catch (IOException e) {
			this.log.debug("ERROR sendPeerKeys in PEER. "+e.getMessage()+" IP:PORT" + p.getIp()+":"+Peer.port);
			//e.printStackTrace();
		}
	}
	
	
	@Override
	public void run() {
		try{
			this.log.info("Anti Entropy sleeping "+this.boottime);
			Thread.sleep(this.boottime);
		} catch (InterruptedException e) {
			this.log.error("ERROR in ACTIVETHREAD sleeping botttime!");
			e.printStackTrace();
		}
		long time = 0;
		int cycle = 0;
		this.log.info("to run anti-entropy?:"+this.running);
		while(this.running){
				this.log.info("Anti Entropy running...");
				try {
					
					this.log.info("Anti Entropy sleeping "+this.interval);
					Thread.sleep(this.interval);
					this.log.info("Anti Entropy waked up ");
					cycle = cycle + 1;
					if(running){ //Treat the case where the Peer was removed and the Thread is assleep. 
						time = time + this.interval;
						this.log.info("Anti Entropy cycle "+cycle+" at time "+time);
						ArrayList<PeerData> myneighbors = view.getSliceLocalView();
						for(PeerData toContact : myneighbors){
							//Contact Peer in order to check if there are missing objects
							HashSet<Long> mykeys = this.store.getKeys();
							this.sendPeerKeys(toContact,mykeys);
							this.log.debug("Anti Entropy request sent to "+toContact.getID()+" with "+mykeys.size()+" keys at cycle "+cycle);
						}

					}
				} catch (InterruptedException e) {
					this.log.error("ERROR in ACTIVETHREAD!");
					e.printStackTrace();
				}
				
			
		}
		
	}

	

}
