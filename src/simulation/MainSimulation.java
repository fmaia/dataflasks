package simulation;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;

import pt.minha.api.Entry;
import pt.minha.api.Host;
import pt.minha.api.Main;
import pt.minha.api.World;
import pt.minha.api.sim.Simulation;
import pt.minha.api.Process;
import utilities.TimeAdvancer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import bootstrapper.Bootstrapper;
import core.Peer;


public class MainSimulation {


	public static long lastid;

	private static World world;
	private static String firstip;
	private static HashMap<String,Entry<Peer>> entrylist;
	//private static HashMap<String,Entry<App>> removed;
	private static HashMap<String,Peer> peers;



	public static void main(String[] args) {

		//generating ids for the peers
		lastid = 0L;

		try {
			world = new Simulation();

			entrylist = new HashMap<String, Entry<Peer>>();
			peers = new HashMap<String,Peer>();

			Properties prop = new Properties();
			prop.load(new FileInputStream("config/config.properties"));
			
			int viewsize = Integer.parseInt(prop.getProperty("viewsize"));
			int number_of_peers = Integer.parseInt(prop.getProperty("number_of_peers"));
			boolean loadfromfile = Boolean.parseBoolean(prop.getProperty("loadfromfile"));
			long timeinterval = Long.parseLong(prop.getProperty("timeinterval"));
			long boottime = Long.parseLong(prop.getProperty("boottime"));
			String peerlistfile = prop.getProperty("peerlistfile");
			int cycles = Integer.parseInt(prop.getProperty("cycles"));
			long psssleepinterval = Long.parseLong(prop.getProperty("psssleepinterval"));
			long pssboottime = Long.parseLong(prop.getProperty("pssboottime"));
			int repmax = Integer.parseInt(prop.getProperty("repmax"));
			int repmin = Integer.parseInt(prop.getProperty("repmin"));
			int maxage = Integer.parseInt(prop.getProperty("maxage"));
			boolean localmessage = Boolean.parseBoolean(prop.getProperty("localmessage"));
			int localinterval = Integer.parseInt(prop.getProperty("localinterval"));
			String loglevel = prop.getProperty("loglevel");
			//ACTIVE THREADS
			boolean testingviewonly = Boolean.parseBoolean(prop.getProperty("testingviewonly"));
			long activeinterval = Long.parseLong(prop.getProperty("activeinterval"));
			float replychance = Float.parseFloat(prop.getProperty("replychance"));
			boolean smart = Boolean.parseBoolean(prop.getProperty("smart"));
			long initload = Long.parseLong(prop.getProperty("initload"));
			long initrun = Long.parseLong(prop.getProperty("initrun"));

			// Bootstrapper
			Host bootstrapperHost = world.createHost();
			Process bootproc = bootstrapperHost.createProcess();
			Entry<Bootstrapper> booter = bootproc.createEntry(bootstrapper.Bootstrapper.class,bootstrapper.BootstrapperImpl.class.getName());
			firstip = bootstrapperHost.getAddress().getCanonicalHostName();
			Bootstrapper boot = booter.call().initBootstrapper(0,viewsize,firstip);
			booter.queue().main(new String[0]);

			Entry<Peer>[] e = world.createEntries(number_of_peers,core.Peer.class,core.PeerImpl.class.getName());


			//forcing uniformity
			double step = 1.0/number_of_peers;
			double start = 0;

			for(int i=0; i<e.length; i++){
				String ip = e[i].getProcess().getHost().getAddress().getCanonicalHostName();
				Double npos = start;
				start = start + step;
				long pid = lastid+1L;
				lastid = pid;
				e[i].queue().initPeer(ip,pid,npos,loadfromfile,firstip,psssleepinterval,
						pssboottime,viewsize,repmax,repmin,maxage,localmessage,localinterval,loglevel,
						testingviewonly,activeinterval,replychance,smart);
				entrylist.put(ip, e[i]);
				boot.addIP(ip,pid,npos);

			}

			// TIMEADVANCER
			Host timehost = world.createHost();
			Process timeproc = timehost.createProcess();
			Entry<TimeAdvancer> time = timeproc.createEntry(utilities.TimeAdvancer.class,utilities.TimeAdvancerImpl.class.getName());
			time.queue().initTimeAdvancer();

			world.runAll(e);

			time.getResult();
			String[] timeargs = new String[2];
			timeargs[0] = new Long(timeinterval).toString();
			timeargs[1] = new Long(boottime).toString();
			time.queue().main(timeargs);


			//YCSB
			if(initload!=0){
				lastid = lastid + 1;
				queueYCSBLoad(lastid,initload);
			}
			if(initrun!=0){
				lastid = lastid + 1;
				queueYCSBRun(lastid,initrun);
			}
			
			//Runing INITIAL PEERS

			//Writing data for Fake Load Balancer
			String peerlist = "";
			//Queuing MAIN for PEERS
			for (Map.Entry<String, Entry<Peer>> val : entrylist.entrySet()){
				//the getResult will go for the result of getPeer queued above returning a reference for the peer
				Peer p = (Peer)val.getValue().getResult();
				peers.put(val.getKey(), p);
				//Writing data for Fake Load Balancer
				peerlist = peerlist + p.getIP() + " " + Peer.port + " " + p.getID() + " " + p.getPOS() +" ";
				//queue peer main
				val.getValue().queue().main(new String[0]);
			}

			//Writing data for Fake Load Balancer
			PrintWriter out = new PrintWriter(peerlistfile);
			out.println(peerlist);
			out.close();

			long startuptime = 0;
			if(boottime!=0){
				startuptime = world.run(boottime,TimeUnit.SECONDS);
			}

			int churnperiodT = 0;

			//Observation Cycles Code
			for(int i=0;i<=cycles;i++){
				churnperiodT = churnperiodT + 1;
				long now = world.run(timeinterval,TimeUnit.SECONDS);
				
				//Logging
				logKeysetAndPSS(now,false);
				
				now = now - startuptime;
			}


			//world.close();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (Throwable e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}



	}

	private static void logKeysetAndPSS(long now, boolean keyset){
		FileWriter fstream;
		try {
			fstream = new FileWriter("logs/keyset/"+(now/1000000000)+".txt");
			BufferedWriter fout = new BufferedWriter(fstream);
			HashMap<Long,Integer> storedKeys = new HashMap<Long,Integer>();
			
			for(Peer pi : peers.values()){
				String s = pi.getPSSLog(); 
				if(!s.equals("")){
					fout.write(s);
					fout.write("\n");
				}
			}
			fout.close();
			
			if(keyset){
				for(Peer pi : peers.values()){
					Long [] tmp = pi.getStoredKeys(now);
					for (Long l : tmp){
						Integer ti = storedKeys.get(l);
						if(ti==null){
							storedKeys.put(l,1);
						}
						else{
							storedKeys.put(l,ti+1);
						}
					}
				}
				//writing keyset to disk for offline processing
				fstream = new FileWriter("logs/keyset/"+(now/1000000000)+"STOREDKEYS.txt");
				fout = new BufferedWriter(fstream);
				for(Long chave :storedKeys.keySet()){
					fout.write(chave+" "+storedKeys.get(chave)+"\n");
				}
				fout.close();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private static void queueYCSBRun(long id,long runtime){
		try{
			Host ycsbhost = world.createHost();
			Process ycsbproc = ycsbhost.createProcess();
			Entry<Main> ycsb = ycsbproc.createEntry();
			ycsb.after(runtime,TimeUnit.SECONDS).queue().main("com.yahoo.ycsb.Client","-t","-s","-threads","1","-db","ycsbglue.StratusClient","-p","exportfile=ycsbRUN.txt",
					"-p","stratus.ip="+ycsbhost.getAddress().getCanonicalHostName(),"-p",
					"stratus.port="+Peer.port,"-p", "stratus.id=ycsbRun","-P", "workloads/workloadb");
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private static void queueYCSBLoad(long id,long initload){
		Host ycsbhost;
		try {
			ycsbhost = world.createHost();
			Process ycsbproc = ycsbhost.createProcess();
			Entry<Main> ycsb = ycsbproc.createEntry();
			ycsb.after(initload,TimeUnit.SECONDS).queue().main("com.yahoo.ycsb.Client","-load","-s","-threads","1","-db","ycsbglue.StratusClient","-p","exportfile=ycsbLOAD.txt",
					"-p","stratus.ip="+ycsbhost.getAddress().getCanonicalHostName(),"-p",
					"stratus.port="+Peer.port,"-p", "stratus.id=ycsbload","-P", "workloads/workloadb");
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
