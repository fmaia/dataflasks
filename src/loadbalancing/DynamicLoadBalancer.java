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
package loadbalancing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.log4j.Logger;

import pss.PSSMessage;
import pss.PSSMessage.TYPE;
import common.PeerData;
import core.Peer;

public class DynamicLoadBalancer implements LoadBalancer, Runnable {

	private ArrayList<PeerData> allnodes;
	private Logger log;
	private Random rnd;
	private boolean running;
	private long interval;
	private DatagramSocket sendersocket;
	private String ip;

	public DynamicLoadBalancer(Logger log, Random rnd,String bootip, String ip, long interval){
		this.allnodes= new ArrayList<PeerData>();
		this.log = log;
		this.rnd = rnd;
		this.running = true;
		this.interval = interval;
		this.ip = ip;
		try {
			this.sendersocket = new DatagramSocket(Peer.lboutport);
		} catch (SocketException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		String targetip = bootip;
		int targetport = Peer.bootport;
		boolean connected = false;
		while(!connected){
			try {
				this.log.info("Load balancer is connecting to "+targetip+":"+targetport+" to retrieve initial view");
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
					this.allnodes.add(new PeerData(peerip,peernslices,peerage,peerslice,peerpos,peermyID));
					npeers = npeers -1;
				}
				serverSocket.close();
				connected = true;
			}
			catch(Exception e){
				this.log.error("ERROR conecting to socket. Retrying...");
				try {
					Thread.sleep(this.rnd.nextInt(5000));
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		this.log.debug("DynamicLoadBalancer initialized.");
		
	}
	
	@Override
	public synchronized PeerData getRandomPeer(){
		int listSize = this.allnodes.size();
		int randi = rnd.nextInt(listSize);
		PeerData tmp = this.allnodes.get(randi);
		return tmp;
	}

	@Override
	public void run() {
		while(running){
					
				try {
					Thread.sleep(this.interval);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if(running){ //Treat the case where the peer was stopped but the thread was asleep;
					synchronized(this){
						PSSMessage msgglobal = new PSSMessage(this.allnodes,TYPE.LOADBALANCE,this.ip);
						PeerData target = this.getRandomPeer();
						this.sendMsg(target, msgglobal);
						log.info("LB message sent to "+target);
					}
				}
			
		}
	}

	public synchronized void processMeesage(PSSMessage msg) {
		this.allnodes = msg.list;
	}

	
	private synchronized int sendMsg(PeerData p, PSSMessage psg){
		try {
			
			byte[] toSend = psg.encodeMessage();
			this.log.debug("pss message size = "+toSend.length);
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()),  Peer.pssport);
			this.log.debug("sending message to "+p.getIp()+":"+ Peer.pssport);
			sendersocket.send(packet);	
			this.log.debug("message sent to "+p.getIp()+":"+ Peer.pssport+" Message:"+ packet.toString());
			return 0;
		} catch (IOException e) {
			this.log.error("ERROR sendget in PEER. "+e.getMessage()+" IP:PORT" + p.getIp()+":"+ Peer.pssport);
			//e.printStackTrace();
		}
		return 1;
	}
}
