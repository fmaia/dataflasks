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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import pss.PSS;

import org.apache.log4j.Logger;

import store.KVStore;
import common.PeerData;
import core.Peer;

public class Worker implements Runnable {
	
	
	private static long aeReqCount = 0L;
	
	public static synchronized long getAeReqCount() {
		aeReqCount = aeReqCount + 1;
		return aeReqCount;
	}


	private Logger log;
	private KVStore store;
	private PSS view;
	private Long myid;
	private float chance;
	private Random rnd;
	private boolean smartforward;
	private Message msg;
	private String myip;
	private SenderSocketHandler sockethandler;
	
	public Worker(String ip,Long id,KVStore store, PSS view, float chance,
			boolean smart,Logger log, Random rnd, Message msg, SenderSocketHandler sockethandler){
		this.log = log;
		this.store = store;
		this.view = view;
		this.myid = id;
		this.chance = chance;
		this.rnd = rnd;
		this.smartforward = smart;
		this.msg = msg;
		this.myip = ip;
		this.sockethandler = sockethandler;
	}
	
	private void forwardMessage(ArrayList<PeerData> myview){
		//forwarding the request to every Peer in myview.
		for (PeerData p : myview){
			//long start = System.nanoTime();
			try {
				//Connecting to host
				this.log.debug("TRYING TO FORWARD MSG TO "+p.getID()+" KEY:"+this.msg.key+" MSGTYPE:"+this.msg.messagetype);			
				DatagramSocket socket = this.sockethandler.getSocket();
				byte[] toSend = this.msg.encodeMessage();
				DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()), Peer.port);
				socket.send(packet);		
				this.log.debug("MSG FORWARDED TO "+p.getID()+" KEY:"+this.msg.key+ " REQID"+this.msg.reqid+ " reqISSUER:"+this.msg.id);
				this.sockethandler.returnSocket(socket);
				
			} catch (IOException e) {
				//log.error("Time:"+(System.nanoTime()-start));
				log.error("Worker - forwardRequest PUT - IOException! STACKTRACE:"+e.getMessage()+" "+Arrays.deepToString(e.getStackTrace()));
			}
		}
	}
	
	
	
	private void replyClient(Message replymsg){
		try {
			DatagramSocket socket = this.sockethandler.getSocket();
			byte[] toSend = replymsg.encodeMessage();
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(this.msg.ip), this.msg.port);
			socket.send(packet);	
			this.sockethandler.returnSocket(socket);
		} catch (IOException e) {
			log.error("Worker - replyClient - IOException!" + e.getMessage());
		}
	}
	
	
	private int sendget(String targetip, Long key, String ip, int port){
		try {
			DatagramSocket socket = this.sockethandler.getSocket();
			byte[] toSend = Message.encodeMessageGet(ip,Peer.port,key,"intern"+this.myid+getAeReqCount(),this.myid);
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(targetip), Peer.port);
			socket.send(packet);
			this.log.info("Anti entropy get sent.");
			this.sockethandler.returnSocket(socket);
			return 0;
		} catch (IOException e) {
			this.log.debug("ERROR sendget in PEER. "+e.getMessage()+" IP:PORT" + targetip+":"+Peer.port);
			//e.printStackTrace();
		}
		return 1;
	}
	
	@Override
	public void run() {
		int operation = this.msg.messagetype;
		String ip = this.msg.ip;
		int port = this.msg.port;
		switch (operation) {
		case 2:
			//PUT operation
			this.log.info("Received Put Operation in Worker thread. key:value -> "+ this.msg.key + " : "+ this.msg.value );
			long key = this.msg.key;
			byte[] value = this.msg.value;
			//Storing value if it should be stored
			if(!this.store.haveseen(key)){
				boolean stored = this.store.put(key, value);
				if(stored){
					log.info("Stored key:"+key);
					float achance = this.rnd.nextFloat();
					ArrayList<PeerData> myview = this.view.getSliceLocalView();
					if(achance<=this.chance){
						//if this node stored the value should reply the client signaling such operation
						Message replymsg = new Message(11,this.myip,Peer.port,null,null,key,this.myid);
						this.replyClient(replymsg);
						//forwarding the request to other peers IN MY SLICE if it is a new obj.
						this.log.info("stored and tried to reply to client "+ip+":"+port+" key "+key);
						this.forwardMessage(myview);
					}
					else{
						this.log.info("STORED BUT CHANCE ("+achance+") DICTATED HOST WONT REPLY key:"+key);
						this.forwardMessage(myview);
						this.log.info("FORWARD AFTER CHANCE DICTATED IT WOULD NOT REPLY. ("+myview.size()+")");
					}
				}
				else{
					this.log.info("STORED=FALSE smart:"+this.smartforward);
					if(this.smartforward){
						this.log.info("entered smart forwarding task");
						int objslice = this.store.getSliceForKey(key);
						this.log.info("this object target partition:"+objslice);
						ArrayList<PeerData> lsp = this.view.havePeerFromSlice(objslice);
						this.log.info("Entered SMART for partition: "+objslice+ " number of peers:"+lsp.size());
						if(lsp.isEmpty()){
							//forwarding the request to other peers RANDOMLY as I don't have peers of such slice in my list.
							ArrayList<PeerData> myview = this.view.getView();
							this.log.info("NOT STORED HERE Going to forward the PUT request. key:"+key);
							this.forwardMessage(myview);
						}
						else{
							//forwarding the request to peers of the intended slice
							this.log.info("NOT STORED HERE Going to forward the PUT request SMART WAY. key:"+key);
							this.forwardMessage(lsp);
						}
					}
					else{
						//forwarding the request to other peers RANDOMLY if it is a new obj.
						ArrayList<PeerData> myview = this.view.getView();
						this.log.info("NOT STORED HERE Going to forward the PUT request. key:"+key);
						this.forwardMessage(myview);
					}
				}
			}
			else{
				this.log.info("Worker IGNORED PUT OPERATION for key:"+key);
			}
			break;
		case 3:
			//GET operation
			this.log.info("Received get operation in Worker thread: reqid:"+this.msg.reqid+ " Key:"+this.msg.key);
			String requestid = this.msg.reqid;
			long requestedkey = this.msg.key;
			byte[] temp = null;
			//Check if this is a duplicate request
			this.log.debug("GET: "+this.store.inLog(requestid));
			if(!this.store.inLog(requestid) && !requestid.startsWith("intern")){
				this.store.logreq(requestid);
				temp = this.store.get(requestedkey);
				if(temp!=null){
					//Send value to Client
					float achance = this.rnd.nextFloat();
					if(achance<=this.chance){
						Message replymsg = new Message(10,this.myip,Peer.port,temp,requestid,requestedkey,this.myid);
						this.replyClient(replymsg);
						this.log.debug("GET RECEIVED AND REPLIED req_id:"+requestid);
						//Already replied to Client so no need to forward the request
					}
					else{
						this.log.debug("GET RECEIVED BUT CHANCE DICTATED IT WOULND'T REPLY req_id:"+requestid);
						int objslice = this.store.getSliceForKey(requestedkey);
						ArrayList<PeerData> lsp = this.view.havePeerFromSlice(objslice);
						//Do not hold the value - need to forward the request.
						if(this.smartforward && !lsp.isEmpty()){
							this.forwardMessage(lsp);
						}
						else{
							ArrayList<PeerData> myview = this.view.getView();
							this.forwardMessage(myview);
						}
					}
				}
				else{
					int objslice = this.store.getSliceForKey(requestedkey);
					ArrayList<PeerData> lsp = this.view.havePeerFromSlice(objslice);
					//Do not hold the value - need to forward the request.
					if(this.smartforward && !lsp.isEmpty()){
						this.log.debug("GET RECEIVED BUT HOST DOES NOT HOLD VALUE FORWARDING SMART WAY. req_id:"+requestid);
						this.forwardMessage(lsp);
					}
					else{
						this.log.debug("GET RECEIVED BUT HOST DOES NOT HOLD VALUE FORWARDING. req_id:"+requestid);
						ArrayList<PeerData> myview = this.view.getView();
						this.forwardMessage(myview);
					}

				}
			}
			else{
				//Special internal get for anti-entropy
				if(requestid.startsWith("intern")){
					this.log.info("PassiveThread Received Anti-Entropy reply.");
					if(!this.store.aeIsInLog(requestid)){ //Check if this request was already seen by the peer. In that case, ignore it.
						this.store.aeLog(requestid);
						temp = this.store.get(requestedkey);
						if(temp!=null){
							//Send value to Client
							Message replymsg = new Message(10,this.myip,Peer.port,temp,requestid,requestedkey,this.myid);
							this.replyClient(replymsg);
							this.log.info("ANTI ENTROPY GET RECEIVED AND REPLIED req_id:"+requestid);
							//Already replied to Client so no need to forward the request
						}
						else{
							//Do not hold the value - need to forward the request.
							this.log.info("ANTI ENTROPY GET RECEIVED BUT HOST DOES NOT HOLD VALUE FORWARDING. req_id:"+requestid);
							ArrayList<PeerData> myview = this.view.getView();
							this.forwardMessage(myview);
						}
					}
				}
			}
			break;
		case 4:
			//Exchange Operation
			this.log.info("PassiveThread Received Anti-Entropy request. Received "+msg.keys.size()+" keys.");
			HashSet<Long> mykeys = this.store.getKeys();
			HashSet<Long> toRequest = new HashSet<Long>();
			if(mykeys.isEmpty()){
				for(Long l : msg.keys){
					toRequest.add(l);
				}
			}
			else{
				for(Long l : msg.keys){
					if(!mykeys.contains(l)){
						toRequest.add(l);
					}
				}
			}
			this.log.info("Anti entropy - going to ask for keys to "+this.msg.id+" keystoRequest:"+toRequest.size());
			for (Long k : toRequest){
				sendget(this.msg.ip,k,this.myip,Peer.port);
			}
			break;
		case 10:
			//Reply to Exchange Gets
			this.log.info("Value with key:"+msg.key+" received in antiEntropy mechanism");
			this.store.put(msg.key, msg.value);

			break;
			
			
		default: break;

		}
		this.log.info("Worker finished.");

	}

}
