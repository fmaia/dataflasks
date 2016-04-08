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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;

import pt.haslab.dataflasks.common.DFLogger;
import pt.haslab.dataflasks.core.Peer;
import pt.haslab.dataflasks.messaging.*;
import pt.haslab.dataflasks.pss.PSS;
import pt.haslab.dataflasks.store.KVStore;

public class PassiveThread implements Runnable {

	private DatagramSocket ss;
        private ReliableServerSocket rss;
        private ReliableSocket rs;
	private boolean running;
	private DFLogger log;
	private KVStore store;
	private PSS view;
	private Long myid;
	private String myip;
	private float chance;
	private boolean smart;
	private Random rnd;
	
	private ExecutorService exService;
	private SenderSocketHandler sockethandler;
	
	public PassiveThread(Long id,KVStore store, PSS view,String ip,int port,float chance, boolean smart, Random rnd,DFLogger log,long wait){
		this.running = true;
		this.log = log;
		this.store = store;
		this.view = view;
		this.myid = id;
		this.myip = ip;
		this.chance = chance;
		this.smart = smart;
		this.rnd = rnd;
		try {
			Thread.sleep(wait);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		this.log.info("Antes do executor.");
		this.exService = Executors.newFixedThreadPool(1);
		this.log.info("Antes do socket handler.");
		this.sockethandler = new SenderSocketHandler(1,this.log);
		this.log.info("PassiveThread Initialized.");
		
		try {
			this.ss = new DatagramSocket(port,InetAddress.getByName(ip));
                        this.rss = new ReliableServerSocket(this.ss,Peer.maxCliQueue);
		} catch (IOException e) {
			log.error("PassiveThread ERROR in constructor!");
		}
	}

	public void stop(){
		//this.ss.close();
		this.running = false;
	}
	
	
	private MessageInterface readMessage(byte[] data, MessageType type){
		MessageInterface res = null;
		switch(type){
		case GET:
			res = new GetMessage();
			res.decodeMessage(data);
			return res;
		case PUT:
			res = new PutMessage();
			res.decodeMessage(data);
			return res;
		case REPLICAMAINTENANCE:
			res = new ReplicaMaintenanceMessage();
			res.decodeMessage(data);
			return res;
		case GETREPLY:
			res = new GetReplyMessage();
			res.decodeMessage(data);
			return res;
		case PUTREPLY:
			res = new PutReplyMessage();
			res.decodeMessage(data);
			return res;
		case MISSINGHASHREQ:
			res = new DedupReplicaRequestMessage();
			res.decodeMessage(data);
			return res;
		case DEDUPPUT:
			res = new DedupReplicaPutMessage();
			res.decodeMessage(data);
			return res;
		default:
			return res;
		}
	}
	
	public void run() {
		//Waits for incoming packets and asks Worker to process them.
		while (running) {
			try {
                                log.debug("PassiveThread: waiting gor connection at "+this.rss.getLocalSocketAddress()+":"+this.rss.getLocalPort());
				this.rs = (ReliableSocket) this.rss.accept();
                                log.debug("PasssiveThread: Received connection from Peer "+this.rs.getRemoteSocketAddress());
                                InputStream is = rs.getInputStream();
                                DataInputStream diss = new DataInputStream(is);
                                int size = diss.readInt();
                                byte [] data = new byte[size];
                                diss.readFully(data);
                                diss.close();
				//DatagramPacket packet = new DatagramPacket(new byte[65500],65500);
				//log.debug("PASSIVE waiting for packet....");
				//ss.receive(packet);
				//byte[] data = packet.getData();
				log.debug("PASSIVE packet received with size "+data.length);
                                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
				int messagetype = dis.readInt();
				dis.close();
				log.debug("PASSIVE packet received with type number "+messagetype);
				MessageInterface msg = readMessage(data,MessageType.getType(messagetype));
				log.debug("PassiveThread Message Received of type:"+msg.getMessageType());
				this.exService.submit(new Worker(myip,myid,this.store,this.view,this.chance,this.smart,this.log,this.rnd,msg,this.sockethandler));
				//new Thread().start();;
				log.debug("PASSIVE worker thread launched....");
				
			} catch (SocketException e) {
				log.info("PassiveThread Server disconnected!");
				e.printStackTrace();
			} catch (IOException e) {
				log.error("PassiveThread ERROR in run()!");
				e.printStackTrace();
			}
			catch (Throwable e){
				log.error("PassiveThread ERROR throwable.");
				e.printStackTrace();
			}
		}	

	}

}
