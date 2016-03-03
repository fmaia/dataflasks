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
package pt.haslab.dataflasks.pss;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pt.haslab.dataflasks.common.DFLogger;

import pt.minha.models.global.Debug;
import pt.haslab.dataflasks.core.Peer;



public class PSSThread extends Thread {
	
	public DFLogger log;
	private DatagramSocket ss;
	private boolean running;
	private PSS pss;
	
	private ExecutorService exService;
	
	public PSSThread(PSS pss,String ip,DFLogger log,long wait){
		this.pss = pss;
		this.log = log;
		try {
			Thread.sleep(wait);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		this.log.info("PSSThread before executor.");
		try{
		this.exService = Executors.newFixedThreadPool(1);
		}
		catch(Throwable e){
			Debug.println(e.getMessage());
			e.printStackTrace();
		}
		this.log.info("PSSThread after executor.");
		try {
			this.ss = new DatagramSocket(Peer.pssport,InetAddress.getByName(ip));
		} catch (IOException e) {
			log.error("PSSThread ERROR in constructor!");
		}
		this.log.info("PSSThread after datagramsocket.");
		this.running = true;
		this.log.info("PSSThread initialized.");
	}
	
	private class PSSWorker extends Thread{
		private byte[] p;
		private DFLogger log;
		private PSS pss;
		
		public PSSWorker(byte[] p, PSS pss,DFLogger log){
			this.p = p;
			this.pss = pss;
			this.log = log;	
		
		}
		
		public void run(){
			PSSMessage msg;
			try {
				msg = new PSSMessage(p,this.log);
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
		//this.ss.close();
		//this.log.info("PSSThread stopped.");
	}
	//-----------------
	
	@Override
	public void run() {
		this.log.info("PSSThread run...");
		//Waits for incoming packets and asks Worker to process them.
		while (running) {
			try {
				DatagramPacket packet = new DatagramPacket(new byte[65500],65500);
				log.debug("PSSThread waiting for packet....");
				ss.receive(packet);
				byte[] data = packet.getData();
				log.debug("PSSThread packet received with size "+packet.getLength());
				log.debug("Packet received. Assigning a worker for the job.");
				this.exService.submit(new PSSWorker(data,this.pss,this.log));
				log.debug("PSS packet received....");
				
			} catch (SocketException e) {
				log.info("PSSThread Server disconnected!");
			} catch (IOException e) {
				log.error("PSSThread ERROR in run()!");
			}
		}	

	}
	
}
