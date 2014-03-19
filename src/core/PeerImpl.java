package core;

import java.io.IOException;

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
	
	//RUN OUTSIDE SIMULATION ONLY
	
	
	@Override
	public Peer initPeer(String ip,long id,double position,boolean loadfromfile,String bootip, 
			long psssleepinterval, long pssboottime, int pssviewsize, int repmax, 
				int repmin, int maxage, boolean localmessage, int localinterval,String loglevel) {
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
		return this;
	}

	@Override
	public void main(String[] args) {
		
		
		
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
			GroupConstruction flasks = new GroupConstruction(this.id,this.position,
								this.repmin,this.repmax,this.maxage,this.localmessage,
													this.localinterval,this.store,this.log);
			PSS cyclon = new PSS(this.bootip,this.ip,this.id,this.pssSleepInterval,
									this.pssboottime,this.pssviewsize,this.log,flasks);
			PSSThread pssthread = new PSSThread(cyclon,this.ip,this.log);
			pssthread.start();
			cyclon.start();
		}
		
		
		
		//STARTING DATA REQUESTS HANDLING COMPONENTS

	}

	//RUN OUTSIDE SIMULATION ONLY
	
	
	@Override
	public Long[] getStoredKeys(long now) {
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub

	}

	@Override
	public String getPSSLog() {
		
		return null;
	}

	//---------------------------------------------------
	
}
