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
import java.net.SocketException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import pss.PSS;
import store.KVStore;

public class PassiveThread implements Runnable {

	private DatagramSocket ss;
	private boolean running;
	private Logger log;
	private KVStore store;
	private PSS view;
	private Long myid;
	private String myip;
	private float chance;
	private boolean smart;
	private Random rnd;
	
	private ExecutorService exService;
	
	public PassiveThread(Long id,KVStore store, PSS view,String ip,int port,float chance, boolean smart, Random rnd,Logger log){
		this.running = true;
		this.log = log;
		this.store = store;
		this.view = view;
		this.myid = id;
		this.myip = ip;
		this.chance = chance;
		this.smart = smart;
		this.rnd = rnd;
		
		this.exService = Executors.newFixedThreadPool(5);
		this.log.info("PassiveThread Initialized.");
		
		try {
			this.ss = new DatagramSocket(port,InetAddress.getByName(ip));
		} catch (IOException e) {
			log.error("PassiveThread ERROR in constructor!");
		}
	}

	public void stop(){
		this.ss.close();
		this.running = false;
	}
	
	@Override
	public void run() {
		//Waits for incoming packets and asks Worker to process them.
		while (running) {
			try {
				
				DatagramPacket packet = new DatagramPacket(new byte[10000],10000);
				log.debug("PASSIVE waiting for packet....");
				ss.receive(packet);
				byte[] data = packet.getData();
				log.debug("PASSIVE packet received with size "+packet.getLength());
				Message msg = new Message(data);
				log.info("PassiveThread Message Received of type:"+msg.messagetype);
				this.exService.submit(new Worker(myip,myid,this.store,this.view,this.chance,this.smart,this.log,this.rnd,msg));
				log.debug("PASSIVE worker thread launched....");
				
			} catch (SocketException e) {
				log.info("PassiveThread Server disconnected!");
			} catch (IOException e) {
				log.error("PassiveThread ERROR in run()!");
			} 
		}	

	}

}
