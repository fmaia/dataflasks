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
package pt.haslab.dataflasks.loadbalancing;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;


import pt.haslab.dataflasks.common.DFLogger;

import pt.haslab.dataflasks.pss.PSSMessage;
import pt.haslab.dataflasks.core.Peer;



public class LBPassiveThread extends Thread {
	
	public DFLogger log;
	private DatagramSocket ss;
        private ReliableServerSocket rss;
        private ReliableSocket rs;
	private boolean running;
	private DynamicLoadBalancer dlb;

	
	public LBPassiveThread(DynamicLoadBalancer dlb,String ip,DFLogger log){
		this.dlb = dlb;
		this.log = log;
		try {
			this.ss = new DatagramSocket(Peer.lbport,InetAddress.getByName(ip));
                        this.rss = new ReliableServerSocket(this.ss,Peer.maxCliQueue);
		} catch (IOException e) {
			log.error("LBPassiveThread ERROR in constructor!");
		}
		this.log.info("LBPassiveThread after datagramsocket.");
		this.running = true;
		this.log.info("LBPassiveThread initialized.");
	}
	


	//Used outside simulation!!!
	public void stopThread(){
		this.running = false;
		//this.ss.close();
		this.log.info("LBPassiveThread stopped.");
	}
	//-----------------
	
	@Override
	public void run() {
		this.log.info("LBPassiveThread run...");
		//Waits for incoming packets and asks Worker to process them.
		while (running) {
			try {
                                log.debug("LBPassive: waiting gor connection at "+this.rss.getLocalSocketAddress()+":"+this.rss.getLocalPort());
                                this.rs = (ReliableSocket) this.rss.accept();
                                log.debug("LBPassiveThread: Received connection from Peer "+this.rs.getRemoteSocketAddress());
                                InputStream is = rs.getInputStream();
                                DataInputStream diss = new DataInputStream(is);
                                int size = diss.readInt();
                                byte [] data = new byte[size];
                                diss.readFully(data);
                                diss.close();
				PSSMessage msg = new PSSMessage(data,this.log);
				this.dlb.processMeesage(msg);
				
			} catch (SocketException e) {
				log.info("LBPassiveThread Server disconnected!");
			} catch (IOException e) {
				log.error("LBPassiveThread ERROR in run()!");
			}
		}	

	}
	
}
