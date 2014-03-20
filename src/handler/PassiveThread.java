package handler;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
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
		
		this.exService = Executors.newFixedThreadPool(100);
		
		try {
			this.ss = new DatagramSocket(port,InetAddress.getByName(ip));
		} catch (IOException e) {
			log.error("PassiveThread ERROR in constructor!");
		}
	}

	/*
	public void stop(){
		running = false;
		try{
		
		ss.getChannel().close();
		ss.close();
		}
		catch(Exception e){
			log.debug("ERROR in PASSIVE THREAD STOP");
		}
		log.debug("PassiveThread stop invoked.");
	}*/
	//-------------------------------------------------------------------
	
	@Override
	public void run() {
		//Waits for incoming packets and asks Worker to process them.
		while (running) {
			try {
				DatagramPacket packet = new DatagramPacket(new byte[1000],1000);
				log.debug("PASSIVE waiting for packet....");
				ss.receive(packet);
				log.debug("PASSIVE packet received....");
				Message msg = new Message(packet.getData());
				log.info("MESSAGE RECIEVED of TYPE:"+msg.messagetype);
				if(msg.messagetype==23){ //Shutdown message
					running = false;
				}
				else{
					this.exService.submit(new Worker(myip,myid,this.store,this.view,this.chance,this.smart,this.log,this.rnd,msg));
					log.info("PASSIVE worker thread launched....");
				}
			} catch (Exception e) {
				log.error("PassiveThread ERROR in run()!");
			}
		}	

	}

}
