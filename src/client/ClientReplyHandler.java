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
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

import common.DFLogger;

import handler.Message;




public class ClientReplyHandler implements Runnable {

	private boolean running = true;
	private DatagramSocket ss;
	private DFLogger log;
	private int myPort;
	private String myIp;
	private HashMap<String,byte[]> getReplies;
	private HashMap<Long,Set<Long>> putReplies;
	private int putreps;
	private long waitTimeout;
	
	public ClientReplyHandler(String ip,int port,int nputreps,long waittimeout,DFLogger log){
		this.myIp=ip;
		this.myPort=port;
		this.log = log;
		this.getReplies = new HashMap<String,byte[]>();
		this.putReplies = new HashMap<Long,Set<Long>>();
		this.putreps = nputreps;
		this.waitTimeout = waittimeout;
		try{
			this.ss = new DatagramSocket(myPort,InetAddress.getByName(myIp));;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void stop(){
		this.running = false;
		this.ss.close();
	}
	
	public void registerGet(String req_id){
		synchronized(this.getReplies){
			this.getReplies.put(req_id, "null".getBytes() );
		}
	}
	
	public byte[] waitForGet(String req_id){
		byte[] res = null;
		synchronized(this.getReplies){
			byte[] valuebytes = null;
			String value = "null";
			try {
				this.getReplies.wait(this.waitTimeout);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			valuebytes = this.getReplies.get(req_id);
			value = new String(valuebytes);
			if(!value.equals("null")){
				res = valuebytes;
				//Cleaning table:
				this.getReplies.remove(req_id);
			}
		}
		return res;
	}
	
	
	public void registerPut(Long key){
		synchronized(this.putReplies){
			Set<Long> repliers = this.putReplies.get(key);
			if(repliers==null){
				repliers = new TreeSet<Long>();
			}
			this.putReplies.put(key,repliers);
		}
	}
	
	public Set<Long> waitForPut(Long key){
		Set<Long> res = null;
		synchronized(this.putReplies){
			Set<Long> repliers = this.putReplies.get(key);
			while(repliers.size()<this.putreps){
				try {
					this.putReplies.wait(this.waitTimeout);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				repliers = this.putReplies.get(key);
			}
			res = repliers;
			//Cleaning table:
			this.putReplies.remove(key);
		}
		return res;
	}
	

	
	@Override
	public void run() {
		//Waits for incoming connections and processes them.
				while (this.running) {
					try {
						DatagramPacket packet = new DatagramPacket(new byte[1000],1000);
						ss.receive(packet);
						Message msg = new Message(packet.getData());
						int op = msg.messagetype;
						switch(op){
						case 10:
							String requestid = msg.reqid;
							long replierid = msg.id;
							byte[] value = msg.value;
							synchronized(this.getReplies){
								if(this.getReplies.containsKey(requestid)){
									this.getReplies.put(requestid, value);
									this.getReplies.notifyAll();
									log.debug("GET reply received from:"+replierid+" REQID "+requestid);
								}
								else{
									log.debug("GET REPLY IGNORED from "+replierid+" REQID "+requestid);
								}
							}
							break;
						case 11:
							long key = msg.key;	
							long replierid1 = msg.id;
							synchronized(this.putReplies){
								Set<Long> current = this.putReplies.get(key);
								if(current==null){
									log.debug("PUT REPLY IGNORED from "+replierid1+" KEY "+key);
								}
								else{
									current.add(replierid1);
									this.putReplies.put(key, current);
									this.putReplies.notifyAll();
									log.debug("PUT reply received from:"+replierid1+" KEY "+key);
								}
							}
							break;
						default: 
							log.debug("CLIENTMSG Operation unrecognized. Going to wait for others.");
							break;
						}
						
						
						//log.debug("CLIENTMSG END OF REPLY PROCESSING");
						
						
					} catch (IOException e) {
						log.error("ClientReplyHandler ERROR in run()!");
					}
				}	

	}

}
