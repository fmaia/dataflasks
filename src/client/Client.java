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
package client;


import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Set;

import loadbalancing.LoadBalancer;
import handler.Message;

import org.apache.log4j.Logger;

import common.PeerData;
import core.Peer;


public class Client implements PLAPI {

	private String myip;
	private int myport;
	private Logger log;
	private LoadBalancer lb;
	private Long requestcount;
	private ClientReplyHandler handler;
	private String myid;
	
	public Client(String id,LoadBalancer lb,String ip,int port,int nputreps,Logger log){
		this.log = log;
		this.myip = ip;
		this.myport = port;
		this.lb = lb;
		this.requestcount = 0L;
		this.myid = id; 
		this.handler = new ClientReplyHandler(ip, port,nputreps,log);
		new Thread(handler).start();
		log.info("Client STARTED. IP:"+myip+" PORT:"+myport);
	}
	
	public synchronized void stop(){
		this.handler.stop();
	}
	
	private int sendput(PeerData p, Long key, byte[] value){

		try {
			DatagramSocket socket = new DatagramSocket();
			byte[] toSend = Message.encodeMessagePut(this.myip,this.myport,key,value,Long.parseLong(myid));
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()), Peer.port);
			socket.send(packet);		
			socket.close();
			return 0;
		} catch (IOException e) {
			this.log.debug("ERROR sendput Client. "+e.getMessage()+" IP:PORT" + p.getIp()+":"+Peer.port);
			//e.printStackTrace();
		}
		return 1;
	} 
	
	
	private int sendget(PeerData p, Long key, String requestid){
		try {
			DatagramSocket socket = new DatagramSocket();
			byte[] toSend = Message.encodeMessageGet(this.myip,this.myport,key,requestid,Long.parseLong(myid));
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()), Peer.port);
			socket.send(packet);		
			socket.close();
			return 0;
		} catch (IOException e) {
			this.log.debug("ERROR sendget Client. "+e.getMessage()+" IP:PORT" + p.getIp()+":"+Peer.port);
			//e.printStackTrace();
		}
		return 1;
	} 
	
	public synchronized void snapshot(PeerData p){
		try {
			DatagramSocket socket = new DatagramSocket();
			byte[] toSend = new Message(4,this.myip,this.myport,Long.parseLong(myid)).encodeMessage();
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()),Peer.port);
			socket.send(packet);		
			this.log.debug("SNAPSHOT REQUEST SENT TO "+p.getID());
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized Long getRequestcount() {
		this.requestcount = this.requestcount+1;
		return this.requestcount;
	}


	public synchronized void setRequestcount(Long requestcount) {
		this.requestcount = requestcount;
	}


	@Override
	public synchronized Set<Long> put(long key, byte[] data) {
		PeerData p = this.lb.getRandomPeer();
		this.handler.registerPut(key);
		int res = this.sendput(p, key, data);
		if(res==0){ //success
			this.log.debug("PUT to "+p.getID()+" KEY "+key+" WAITING FOR REPLY");
			return this.handler.waitForPut(key);
		}
		else{
			this.log.debug("PUT FAILED");
			return null;
		}
	}

	@Override
	public synchronized byte[] get(long nodeID, long key) {
		PeerData p = this.lb.getRandomPeer();
		Long req_id = this.getRequestcount();
		String thisreq = this.myip+":"+this.myport + req_id.toString();
		this.handler.registerGet(thisreq);
		int res = this.sendget(p,key,thisreq);
		if(res==0){
			this.log.debug("GET to "+p.getID()+" KEY "+key+" REQID "+thisreq+" WAITING FOR REPLY");
			return this.handler.waitForGet(thisreq);
		}
		else{
			this.log.debug("GET FAILED");
			return null;
		}
	}

	@Override
	public synchronized byte[] delete(long nodeID, long key) {
		// TODO Auto-generated method stub
		return null;
	}

}
