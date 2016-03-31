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
package pt.haslab.dataflasks.handler;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import pt.haslab.dataflasks.pss.PSS;

import pt.haslab.dataflasks.common.DFLogger;
import pt.haslab.dataflasks.store.KVDedupStoreFileSystem;
import pt.haslab.dataflasks.store.KVStore;
import pt.haslab.dataflasks.store.StoreKey;
import pt.haslab.dataflasks.common.PeerData;
import pt.haslab.dataflasks.core.Peer;
import pt.haslab.dataflasks.messaging.*;

public class Worker implements Runnable {
	
	
	private static long aeReqCount = 0L;
	
	public static synchronized long getAeReqCount() {
		aeReqCount = aeReqCount + 1;
		return aeReqCount;
	}


	private DFLogger log;
	private KVStore store;
	private PSS view;
	private Long myid;
	private float chance;
	private Random rnd;
	private boolean smartforward;
	private MessageInterface msg;
	private String myip;
	private SenderSocketHandler sockethandler;
	
	public Worker(String ip,Long id,KVStore store, PSS view, float chance,
			boolean smart,DFLogger log, Random rnd, MessageInterface msg, SenderSocketHandler sockethandler){
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
				this.log.debug("TRYING TO FORWARD MSG TO "+p.getID()+" MSGTYPE:"+this.msg.getClass());			
				DatagramSocket socket = this.sockethandler.getSocket();
				byte[] toSend = this.msg.encodeMessage();
				DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(p.getIp()), Peer.port);
				socket.send(packet);		
				this.log.debug("MSG FORWARDED TO "+p.getID()+" KEY: "+(this.msg.getMessageKey()));
				this.sockethandler.returnSocket(socket);
				
			} catch (IOException e) {
				//log.error("Time:"+(System.nanoTime()-start));
				log.error("Worker - forwardRequest PUT - IOException! STACKTRACE:"+e.getMessage()+" "+Arrays.deepToString(e.getStackTrace()));
			}
		}
	}
	
	
	
	private void replyClient(MessageInterface replymsg){
		try {
			DatagramSocket socket = this.sockethandler.getSocket();
			byte[] toSend = replymsg.encodeMessage();
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(this.msg.getMessageIP()), this.msg.getMessagePort());
			socket.send(packet);	
			this.sockethandler.returnSocket(socket);
		} catch (IOException e) {
			log.error("Worker - replyClient - IOException!" + e.getMessage());
		}
	}
	
	
	private int sendget(String targetip, Long key, Long version, String ip, int port){
		try {
			DatagramSocket socket = this.sockethandler.getSocket();
			MessageInterface msg = new GetMessage(ip,Peer.port,key,version,"intern"+this.myid+":"+getAeReqCount(),this.myid);
			byte[] toSend = msg.encodeMessage();
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
	
	private int sendhashesget(String targetip, byte[] toSend){
		try {
			DatagramSocket socket = this.sockethandler.getSocket();
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(targetip), Peer.port);
			socket.send(packet);
			this.log.info("Missing hash get sent.");
			this.sockethandler.returnSocket(socket);
			return 0;
		} catch (IOException e) {
			this.log.debug("ERROR sendget in PEER. "+e.getMessage()+" IP:PORT" + targetip+":"+Peer.port);
			//e.printStackTrace();
		}
		return 1;
	}
	
	
	private int sendDedupPut(String targetip, DedupReplicaPutMessage msg){
		try {
			this.log.info("Going to send Dedup put");
			DatagramSocket socket = this.sockethandler.getSocket();
			byte[] toSend = msg.encodeMessage();
			DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(targetip), Peer.port);
			socket.send(packet);
			this.log.info("Dedup Put sent.");
			this.sockethandler.returnSocket(socket);
			return 0;
		} catch (IOException e) {
			this.log.debug("ERROR sendget in PEER. "+e.getMessage()+" IP:PORT" + targetip+":"+Peer.port);
			//e.printStackTrace();
		}
		return 1;
	}

	public void run() {
		MessageType operation = this.msg.getMessageType();
		String ip = this.msg.getMessageIP();
		int port = this.msg.getMessagePort();
		switch (operation) {
		case PUT:
			//PUT operation
			long key = ((PutMessage)this.msg).key;
			long version = ((PutMessage)this.msg).version;
			byte[] value = ((PutMessage)this.msg).value;
			this.log.info("Received Put Operation in Worker thread. key:version -> "+ key + " : "+ version );
			//Storing value if it should be stored
			if(!this.store.haveseen(key,version)){
				boolean stored = this.store.put(key, version, value);
				if(stored){
					log.info("Stored key:"+key);
					float achance = this.rnd.nextFloat();
					ArrayList<PeerData> myview = this.view.getSliceLocalView();
					if(achance<=this.chance){
						//if this node stored the value should reply the client signaling such operation
						MessageInterface replymsg = new PutReplyMessage(this.myip,Peer.port,key,version,this.myid);
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
		case GET:
			//GET operation
			String requestid = ((GetMessage)this.msg).reqid;
			long requestedkey = ((GetMessage)this.msg).key;
			long requestedversion = ((GetMessage)this.msg).version;
			byte[] temp = null;
			this.log.info("Received get operation in Worker thread: reqid:"+requestid+ " Key:"+requestedkey+" Version:"+requestedversion);
			//Check if this is a duplicate request
			this.log.debug("GET: "+this.store.inLog(requestid));
			if(!this.store.inLog(requestid) && !requestid.startsWith("intern")){
				this.store.logreq(requestid);
				temp = this.store.get(new StoreKey(requestedkey,requestedversion));
				if(temp!=null){
					this.log.debug("GET regular request."+temp.toString()+" "+temp.length);
					//Send value to Client
					float achance = this.rnd.nextFloat();
					if(achance<=this.chance){
						MessageInterface replymsg = new GetReplyMessage(this.myip,Peer.port,temp,requestid,requestedkey,requestedversion,this.myid);
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
					this.log.debug("GET regular request. temp is NULL.");
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
					this.log.debug("PassiveThread Received Anti-Entropy reply.");
					if(!this.store.aeIsInLog(requestid)){ //Check if this request was already seen by the peer. In that case, ignore it.
						this.store.aeLog(requestid);
						temp = this.store.get(new StoreKey(requestedkey,requestedversion));
						if(temp!=null){
							//Send value to Client
							MessageInterface replymsg = new GetReplyMessage(this.myip,Peer.port,temp,requestid,requestedkey,requestedversion,this.myid);
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
		case GETREPLY:
			//Reply to Exchange Gets
			GetReplyMessage grmsg = (GetReplyMessage) msg;
			this.log.info("Value with key:"+grmsg.key+" received in antiEntropy mechanism");
			if(!this.store.haveseen(grmsg.key,grmsg.version)){
				this.store.put(grmsg.key, grmsg.version, grmsg.value);
			}
			break;
		case REPLICAMAINTENANCE:
			//Exchange Operation
			HashSet<StoreKey> msgkeys = ((ReplicaMaintenanceMessage)msg).keys;
			this.log.info("PassiveThread Received Anti-Entropy request. Received "+msgkeys.size()+" keys.");
			HashSet<StoreKey> mykeys = this.store.getKeys();
			HashSet<StoreKey> toRequest = new HashSet<StoreKey>();
			if(mykeys.isEmpty()){
				for(StoreKey l : msgkeys){
					toRequest.add(l);
				}
			}
			else{
				for(StoreKey l : msgkeys){
					if(!mykeys.contains(l)){
						toRequest.add(l);
					}
				}
			}
			this.log.info("Anti entropy - going to ask for keys to "+this.msg.getMessageID()+" keystoRequest:"+toRequest.size());
			if(this.store.getClass().equals(KVDedupStoreFileSystem.class)){
				//Dedup mode is on. Just requesting blocks that this node does not currently hold.
				HashMap<StoreKey,ArrayList<String>> missingH = new HashMap<StoreKey,ArrayList<String>>();
				for (StoreKey k : toRequest){
					ArrayList<String> missinghashes = this.store.getMissingHashes(k, ((ReplicaMaintenanceMessage)msg).hashlist.get(k));
					missingH.put(k, missinghashes);
				}
				this.log.info("Anti entropy - going to ask for "+missingH.size()+" missing hash groups");
				MessageInterface reqmessage = new DedupReplicaRequestMessage(this.myip,Peer.port,this.myid,"intern"+this.myid+":"+getAeReqCount(),toRequest,missingH);
				byte[] tosend = reqmessage.encodeMessage();
				sendhashesget(ip,tosend);
			}
			else{
				for (StoreKey k : toRequest){
					sendget(ip,k.key,k.version,this.myip,Peer.port);
				}
			}
			break;
		
		case MISSINGHASHREQ:
			this.log.info("MISSINGHASHREQ received");
			// Some node requested missing blocks. Need to reply with what we have
			// Dedup mode is on
			try{
			HashMap<StoreKey,ArrayList<String>> askedHashes = ((DedupReplicaRequestMessage) this.msg).hashlist;
			this.log.info("Asked Hashes:"+askedHashes.size());
			for(StoreKey file : askedHashes.keySet()){
				//Get missing blocks
				HashMap<String,byte[]> missingblocks = ((KVDedupStoreFileSystem) this.store).getMissingBlocks(file, askedHashes.get(file));
				//Send special put message for this file
				DedupReplicaPutMessage tosend = new DedupReplicaPutMessage(this.myip,Peer.outport,this.myid,missingblocks,file);
				this.sendDedupPut(ip, tosend);
			}
			}
			catch(Throwable e){
				e.printStackTrace();
			}
			break;
		case DEDUPPUT:
			this.log.info("DEDUPPUT received");
			//Receiving missing files from replica maintenance mechanism with dedup mode on.
			((KVDedupStoreFileSystem) this.store).dedupPut(((DedupReplicaPutMessage) msg).key,((DedupReplicaPutMessage) msg).version, ((DedupReplicaPutMessage) msg).blocks);
			break;
		default: 
			this.log.info("Unrecognized message in Worker. Ignoring.");
			break;

		}
		this.log.debug("Worker finished.");

	}

}
