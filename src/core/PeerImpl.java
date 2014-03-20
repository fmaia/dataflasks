package core;

import handler.AntiEntropy;
import handler.PassiveThread;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Random;

import handler.Message;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import pss.PSS;
import pss.PSSThread;
import store.KVStore;

public class PeerImpl implements Peer {

	
	
	//LOCAL VARS
	private long id;
	private String ip;
	private double position;
	private boolean loadfromfile;
	private String bootip;
	
	private Logger log;
	private String loglevel;
	
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
	private boolean testingviewonly;
	private long activeinterval;
	private float replychance;
	private boolean smart;
	
	//RUN OUTSIDE SIMULATION ONLY
	
	
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
		this.loglevel = loglevel;
		this.testingviewonly = testingviewonly;
		this.activeinterval = activeinterval;
		this.replychance = replychance;
		this.smart = smart;
		
		
		return this;
	}

	@Override
	public void main(String[] args) {

		try{
			//SET LOG
			String myself = new Long(id).toString();
			this.log = Logger.getLogger(myself);
			if(this.loglevel.equals("debug")){
				this.log.setLevel(Level.DEBUG);
			}
			else{
				if(this.loglevel.equals("off")){
					this.log.setLevel(Level.OFF);
				}
				else{
					this.log.setLevel(Level.INFO);
				}
			}
			FileAppender capp = null;
			try {
				capp = new FileAppender(new PatternLayout("[%t] %-5p %c %x - %m%n"),"logs/"+myself+".txt");
			} catch (IOException e1) {

				e1.printStackTrace();
			}
			capp.setName(myself);
			this.log.addAppender(capp);

			this.log.debug("Initialized "+myself);

			//START STORE + PSS + GROUP CONSTRUCTION
			this.store = new KVStore(log);
			if(this.loadfromfile){
				this.log.debug("Loading data from file... - NOT IMPLEMENTED!");
			}
			else{
				this.log.debug("Initializing Store, PSS and Group Construction from scratch.");
				this.flasks = new GroupConstruction(this.id,this.position,
						this.repmin,this.repmax,this.maxage,this.localmessage,
						this.localinterval,this.store,this.log);
				this.cyclon = new PSS(this.bootip,this.ip,this.id,this.pssSleepInterval,
						this.pssboottime,this.pssviewsize,this.log,this.flasks);
				this.pssthread = new PSSThread(cyclon,this.ip,this.log);
				pssthread.start();
				cyclon.start();
			}

			//Starting the core threads
			if(!this.testingviewonly){
				active = new AntiEntropy(this.ip,Peer.port,this.id,this.cyclon,this.store,this.activeinterval,new Random(),log);
				Thread tactive = new Thread(active);

				PassiveThread pass = new PassiveThread(this.id,this.store,this.cyclon,this.ip,Peer.port,this.replychance,this.smart,new Random(),log);
				Thread passive = new Thread(pass);

				passive.start();
				tactive.start();

				tactive.join();
				log.info("PEER ACTIVE THREAD FINISHED "+myself);

				//STOPPING passive thread - for now sendind a message. It should be done by .close on the socket when working- minha issue
				DatagramSocket socket = new DatagramSocket();
				byte[] toSend = new Message(23,this.ip,Peer.port,this.id).encodeMessage();
				DatagramPacket packet = new DatagramPacket(toSend,toSend.length,InetAddress.getByName(this.ip), Peer.port);
				socket.send(packet);		
				log.info("Passive Thread Shutdown Requested.");
				socket.close();

				passive.join();
				log.info("PEER PASSIVE THREAD FINISHED "+myself);
				log.info("PEER FINISHED "+myself);
			}

		} catch (Exception e) {
			log.error("ERROR in PeerMAIN. "+e.getMessage());
		}

	}

	//RUN OUTSIDE SIMULATION ONLY
	
	
	@Override
	public Long[] getStoredKeys(long now) {
		return this.store.getStoredKeys();
	}

	@Override
	public void snapshot() {
		// TODO Auto-generated method stub

	}

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
		this.cyclon.stopPSS();
		this.pssthread.stopThread();
		if(!this.testingviewonly){
			this.active.setRunning(false);
		}
		this.log.info("Peer Stopped.");
	}

	@Override
	public String getPSSLog() {
		return this.flasks.getInfo();
	}

	//---------------------------------------------------
	
}
