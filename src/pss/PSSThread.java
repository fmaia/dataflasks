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
package pss;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

import org.apache.log4j.Logger;

import core.Peer;



public class PSSThread extends Thread {
	
	private Logger log;
	private DatagramSocket ss;
	private boolean running;
	private PSS pss;
	
	public PSSThread(PSS pss,String ip,Logger log){
		this.pss = pss;
		this.log = log;
		try {
			this.ss = new DatagramSocket(Peer.pssport,InetAddress.getByName(ip));
		} catch (IOException e) {
			log.error("PSSThread ERROR in constructor!");
		}
		this.running = true;
	}
	
	private class PSSWorker extends Thread{
		private DatagramPacket p;
		private Logger log;
		private PSS pss;
		
		public PSSWorker(DatagramPacket p, PSS pss,Logger log){
			this.p = p;
			this.pss = pss;
			this.log = log;
		}
		public void run(){
			PSSMessage msg;
			try {
				msg = new PSSMessage(this.p.getData(),this.log);
				log.debug("message received from "+msg.sender);
				this.pss.processMessage(msg);
			} catch (Exception e) {
				log.error("PSSWorkerThread ERROR! "+e.getMessage()+" "+e.getLocalizedMessage()+" "+e.getCause());
			}
		}
	}

	//Used outside simulation!!!
	public void stopThread(){
		this.running = false;
		this.ss.close();
		this.log.info("PSSThread stopped.");
	}
	//-----------------
	
	@Override
	public void run() {
		//Waits for incoming packets and asks Worker to process them.
		while (running) {
			try {
				DatagramPacket packet = new DatagramPacket(new byte[5000],5000);
				log.debug("PSSThread waiting for packet....");
				ss.receive(packet);
				log.debug("Packet received. Assigning a worker for the job.");
				Thread work = new PSSWorker(packet,this.pss,this.log);
				work.start();
				log.debug("PSS packet received....");
				
			} catch (SocketException e) {
				log.info("PSSThread Server disconnected!");
			} catch (IOException e) {
				log.error("PSSThread ERROR in run()!");
			}
		}	

	}
	
}
