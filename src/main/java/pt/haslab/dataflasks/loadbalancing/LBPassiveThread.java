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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;


import pt.haslab.dataflasks.common.DFLogger;

import pt.haslab.dataflasks.pss.PSSMessage;
import pt.haslab.dataflasks.core.Peer;



public class LBPassiveThread extends Thread {
	
	public DFLogger log;
	private DatagramSocket ss;
	private boolean running;
	private DynamicLoadBalancer dlb;

	
	public LBPassiveThread(DynamicLoadBalancer dlb,String ip,DFLogger log){
		this.dlb = dlb;
		this.log = log;
		try {
			this.ss = new DatagramSocket(Peer.lbport,InetAddress.getByName(ip));
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
				DatagramPacket packet = new DatagramPacket(new byte[65500],65500);
				log.debug("LBPassiveThread waiting for packet....");
				ss.receive(packet);
				byte[] data = packet.getData();
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
