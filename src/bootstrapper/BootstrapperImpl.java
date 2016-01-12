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
package bootstrapper;


import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import common.DFLogger;

import core.Peer;



public class BootstrapperImpl implements Bootstrapper {

	private DFLogger log;
	private ServerSocket ss;
	private boolean running;
	private HashSet<String> aliveIPs;
	private HashMap<String,Long> aliveIds;
	private HashMap<String,Double> alivePos;
	private ConcurrentLinkedQueue<ArrayList<String>> fila;
	private int viewsize;
	private int initialnodes;
	private String ip;
	private long id;
	

	
	@Override
	public Bootstrapper initBootstrapper(long id,int viewsize,String ip) {
		this.viewsize = viewsize;
		this.ip = ip;
		this.id = id;
		this.aliveIPs = new HashSet<String>();
		this.aliveIds = new HashMap<String,Long>();
		this.alivePos = new HashMap<String,Double>();
		this.fila = new ConcurrentLinkedQueue<ArrayList<String>>();
		this.initialnodes = 10;
		return this;
	}
	
	//Used outside simulation!!!
	public void stopThread(){
		this.running = false;
	}
	
	@Override
	public void addIP(String ip, long id, double pos){
		this.aliveIPs.add(ip);
		this.aliveIds.put(ip, id);
		this.alivePos.put(ip, pos);
	}
	
	public void removeIP(String ip){
		this.aliveIPs.remove(ip);
		this.aliveIds.remove(ip);
		this.alivePos.remove(ip);
	}
	//-----------------
	
	public synchronized Long getIDforIP(String ip){
		return this.aliveIds.get(ip);
	}
	
	public synchronized Double getPosforIP(String ip){
		return this.alivePos.get(ip);
	}
	
	private synchronized void bootfila(int vsize, int n){
		for (int i=0;i<n;i++){
			ArrayList<String> novaVista = this.getViewToSend(vsize);
			this.fila.add(novaVista);
		}
	}
	
	private synchronized ArrayList<String> getViewToSend(int vsize){
		ArrayList<String> res = new ArrayList<String>();
		ArrayList<String> tmp = new ArrayList<String>();
		for(String ip : this.aliveIPs){
			tmp.add(ip);
		}
		if(this.aliveIPs.size()<=vsize){
			res = tmp;
		}
		else{
			Collections.shuffle(tmp,new Random());
			for(String s : tmp){
				if(res.size()<vsize){
					res.add(s);
				}
			}
		}
		return res;
	}
	
	
	public synchronized ArrayList<String> pollFila(){
		ArrayList<String> tosend = this.fila.poll();
		while(tosend==null){
			log.info("No views left.. generating new");
			this.bootfila(viewsize, initialnodes);
			tosend = this.fila.poll();
		}
		return tosend;
	}
	
	private class BootWorker extends Thread{
		private Socket s;
		private DFLogger log;
		private ArrayList<String> tosend;
		private BootstrapperImpl parent;
		
		public BootWorker(Socket p, DFLogger log,BootstrapperImpl parent){
			this.s = p;
			this.log = log;
			this.tosend = parent.pollFila();
			this.parent = parent;
		}
		public void run(){
			
			PrintWriter out;
			try {
				out = new PrintWriter(this.s.getOutputStream(), true);

				int cviewsize = this.tosend.size();

				//out.println(1); //Everybody starts by thinking there is only one partition
				out.println(cviewsize);
				for(String s : this.tosend){
					out.println(s);
					out.println(1);
					out.println(20); //Forcing this data to be forgotten as fast as possible as it is only bootstrap data
					out.println(1);
					out.println(this.parent.getPosforIP(s));
					out.println(this.parent.getIDforIP(s));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			log.debug("Bootstrap Dealt With.");
		}
	}
	
	@Override
	public void main(String[] args) {
		
		
		int myPort = Peer.bootport;
		
		//Configuring Log ------------------------------------------------------------
		String myself = new Long(this.id).toString();
		log = new DFLogger("peer."+myself);
		//Configuring Log ------------------------------------------------------------
	
		
		//Initiating Server
		try {
			this.ss = new ServerSocket(myPort,10000,InetAddress.getByName(this.ip));
		} catch (IOException e) {
			log.error("Bootstraper ERROR in constructor!");
		}
		this.running = true;
		
		this.log.info("Bootstrapper initialized:"+this.aliveIPs.toString());
		while (running) {
			try {
				log.debug("Bootstraper waiting for packet....");
				Socket clientSocket = ss.accept();
				new BootWorker(clientSocket, this.log,this).start();
				log.info("BootWorker launched.");
			} catch (Exception e) {
				log.error("Bootstraper ERROR in run()! "+e.getMessage()+" "+e.getLocalizedMessage()+" "+e.getCause());
			}
		}	
	
	}

}
