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
package core;

import handler.AntiEntropy;
import handler.PassiveThread;

import java.util.Random;

import common.DFLogger;
import pss.PSS;
import pss.PSSThread;
import store.KVStore;
import store.StoreKey;

public class PeerImpl implements Peer {

	
	
	//LOCAL VARS
	private long id;
	private String ip;
	private double position;
	private boolean loadfromfile;
	private String bootip;
	
	private DFLogger log;
	
	private KVStore store;
	private GroupConstruction flasks;
	private PSS cyclon;
	private PSSThread pssthread;
	
	//PSS
	private long pssSleepInterval;
	private long pssboottime;
	private int pssviewsize;
	
	//GROUP CONSTRUCTION
	private int repmax;
	private int repmin;
	private int maxage;
	private boolean localmessage;
	private int localinterval;

	
	//ACTIVE THREADS
	private AntiEntropy active;
	private PassiveThread pass;
	private boolean testingviewonly;
	private long activeinterval;
	private float replychance;
	private boolean smart;
	
	//aux
	private String myself;
	
	public PeerImpl(){
		
	}
	
	@Override
	public Peer initPeer(String ip,long id,double position,boolean loadfromfile,String bootip, 
			long psssleepinterval, long pssboottime, int pssviewsize, int repmax, 
				int repmin, int maxage, boolean localmessage, int localinterval,String loglevel,
				boolean testingviewonly,long activeinterval,float replychance,boolean smart) {
		this.ip = ip;
		this.id = id;
		this.position = position;
		this.loadfromfile = loadfromfile;
		this.bootip = bootip;
		this.repmax = repmax;
		this.repmin = repmin;
		this.maxage = maxage;
		this.localmessage = localmessage;
		this.localinterval = localinterval;
		this.pssSleepInterval = psssleepinterval;
		this.pssboottime = pssboottime;
		this.pssviewsize = pssviewsize;
		this.testingviewonly = testingviewonly;
		this.activeinterval = activeinterval;
		this.replychance = replychance;
		this.smart = smart;
		this.myself = new Long(id).toString();
		
		return this;
	}
	
	@Override
	public Peer initPeerWithData(String ip,long id,boolean loadfromfile,String bootip, 
			long psssleepinterval, long pssboottime, int pssviewsize, int repmax, 
				int repmin, int maxage, boolean localmessage, int localinterval,String loglevel,
				boolean testingviewonly,long activeinterval,float replychance,boolean smart,String[] storedata,String[] groupdata,String[] pssdata) {
		
		this.ip = ip;
		this.id = id;
		this.loadfromfile = loadfromfile;
		this.bootip = bootip;
		this.repmax = repmax;
		this.repmin = repmin;
		this.maxage = maxage;
		this.localmessage = localmessage;
		this.localinterval = localinterval;
		this.pssSleepInterval = psssleepinterval;
		this.pssboottime = pssboottime;
		this.pssviewsize = pssviewsize;
		this.testingviewonly = testingviewonly;
		this.activeinterval = activeinterval;
		this.replychance = replychance;
		this.smart = smart;
		this.myself = new Long(id).toString();
		
		this.store = new KVStore();
		this.store.readFromString(storedata);
		this.flasks = new GroupConstruction(this.store);
		this.flasks.readFromString(groupdata);
		this.position = this.flasks.getPosition();
		if (loglevel.equals("info")) System.out.println("Store and Flasks loaded."+ this.flasks.getInfo());
		this.cyclon = new PSS(this.ip,this.id,this.pssSleepInterval,this.pssboottime,
				this.pssviewsize,this.flasks);
		if (loglevel.equals("info")) System.out.println("Cyclon init. Going to load...");
		this.cyclon.readFromStringList(pssdata);
		System.out.println("Peer "+this.id+" loaded from file.");
		
		return this;
	}

	@Override
	public void main(String[] args) {
		
		try{
			//SET LOG 
			this.log = new DFLogger("peer."+myself);
			this.log.info("Initialized "+myself+" with position;"+this.position);
			long wait = 1000;
			//START STORE + PSS + GROUP CONSTRUCTION
			if(this.loadfromfile){
				//When data is loaded from file logs need to be propagated
				//Logs cannot be initialized when the peer is initiated outside simulation.
				this.log.info("Loaded from file. init logs");
				this.flasks.log = this.log;
				this.cyclon.log = this.log;
				this.log.info("log init done."+this.store.getSlice()+" "+this.flasks.getGroup());
				wait = new Long(new Random().nextInt(20000))+10000;
			}
			else{
				this.log.info("Initializing Store, PSS and Group Construction from scratch.");
				this.store = new KVStore(this.log);
				this.log.info("Initializing PSS and Group Construction from scratch.");
				this.flasks = new GroupConstruction(this.ip,this.id,this.position,
						this.repmin,this.repmax,this.maxage,this.localmessage,
						this.localinterval,this.store,this.log);
				this.cyclon = new PSS(this.bootip,this.ip,this.id,this.pssSleepInterval,
						this.pssboottime,this.pssviewsize,this.log,this.flasks);
				wait = 0;
			}
			
			this.pssthread = new PSSThread(this.cyclon,this.ip,this.log,wait);
			this.pssthread.start();
			this.log.info("PSSThread started.");
			this.cyclon.start();
			this.log.info("PSS started.");
			
			//Starting the core threads
			this.log.info("testingviewonly:"+this.testingviewonly);
			if(!this.testingviewonly){
				this.log.info("Starting data related threads.");
				this.active = new AntiEntropy(this.ip,Peer.port,this.id,this.cyclon,this.store,this.activeinterval,this.pssboottime,new Random(),this.log);
				Thread tactive = new Thread(this.active);
				this.log.info("init antientropy.");
				this.pass = new PassiveThread(this.id,this.store,this.cyclon,this.ip,Peer.port,this.replychance,this.smart,new Random(),this.log,wait);
				Thread passive = new Thread(this.pass);
				this.log.info("init passive thread.");
				passive.start();
				tactive.start();
				this.log.info("Anti entropy and Passive threads started.");
				
				passive.join();
				tactive.join();
				this.log.info("PEER FINISHED "+myself);
			}

		} catch (Exception e) {
			this.log.error("ERROR in PeerMAIN. "+e.getMessage());
		}

	}


	//RUN OUTSIDE SIMULATION ONLY

	@Override
	public long getID() {
		return this.id;
	}
	
	@Override
	public String getIP() {
		return this.ip;
	}
	
	@Override
	public double getPOS() {
		return this.position;
	}

	@Override
	public void stopPeer() {
		//System.out.println("Removing "+this.id);
		this.cyclon.stopPSS();
		this.pssthread.stopThread();
		if(!this.testingviewonly){
			this.active.stop();
			this.pass.stop();
			//this.log.info("Active and Passive threads stopped.");
		}
		//this.log.info("Peer Stopped.");
	}

	@Override
	public String getPSSLog() {
		return this.flasks.getInfo();
	}

	@Override
	public StoreKey[] getStoredKeys() {
		StoreKey[] list = this.store.getStoredKeys();
		return list;
	}

	@Override
	public String getStoreSnap() {
		return this.store.toString();
	}

	@Override
	public String getPSSSnap() {
		return this.cyclon.toString();
	}

	@Override
	public String getGroupConstructionSnap() {
		return this.flasks.toString();
	}


	//---------------------------------------------------
	
}
