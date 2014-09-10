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
package simulation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;

import pt.minha.api.ContainerException;
import pt.minha.api.Entry;
import pt.minha.api.Host;
import pt.minha.api.Main;
import pt.minha.api.World;
import pt.minha.api.sim.Simulation;
import pt.minha.api.Process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import bootstrapper.Bootstrapper;
import core.Peer;


public class MainSimulation {


	public static long lastid;

	private static World world;
	private static String firstip;
	private static HashMap<String,Entry<Peer>> entrylist;
	private static HashMap<String,Peer> peers;
	private static HashMap<Integer,ArrayList<String>> idealconfig;
	//Bootstrapper
	private static Bootstrapper boot;
	
	//Peer Configs
	private static int viewsize;
	private static int number_of_peers;
	private static boolean loadfromfile;
	private static long timeinterval;
	private static long boottime;
	private static String peerlistfile;
	private static int cycles;
	private static long psssleepinterval;
	private static int repmax;
	private static int repmin;
	private static int maxage;
	private static boolean localmessage;
	private static int localinterval;
	private static String loglevel;
	//ACTIVE THREADS
	private static boolean testingviewonly;
	private static long activeinterval;
	private static float replychance;
	private static boolean smart;



	public static void main(String[] args) {

		//generating ids for the peers
		lastid = 0L;

		try {
			world = new Simulation();

			entrylist = new HashMap<String, Entry<Peer>>();
			peers = new HashMap<String,Peer>();
			
			idealconfig = new HashMap<Integer,ArrayList<String>>();

			Properties prop = new Properties();
			prop.load(new FileInputStream("config/config.properties"));
			
			viewsize = Integer.parseInt(prop.getProperty("viewsize"));
			number_of_peers = Integer.parseInt(prop.getProperty("number_of_peers"));
			loadfromfile = Boolean.parseBoolean(prop.getProperty("loadfromfile"));
			timeinterval = Long.parseLong(prop.getProperty("timeinterval"));
			boottime = Long.parseLong(prop.getProperty("boottime"));
			peerlistfile = prop.getProperty("peerlistfile");
			cycles = Integer.parseInt(prop.getProperty("cycles"));
			cycles = (int) (cycles * timeinterval);
			psssleepinterval = Long.parseLong(prop.getProperty("psssleepinterval"))*1000L;
			repmax = Integer.parseInt(prop.getProperty("repmax"));
			repmin = Integer.parseInt(prop.getProperty("repmin"));
			maxage = Integer.parseInt(prop.getProperty("maxage"));
			localmessage = Boolean.parseBoolean(prop.getProperty("localmessage"));
			localinterval = Integer.parseInt(prop.getProperty("localinterval"));
			loglevel = prop.getProperty("loglevel");
			//DATA SNAPSHOT
			long snapshottime = Long.parseLong(prop.getProperty("snapshot"));
			//ACTIVE THREADS
			testingviewonly = Boolean.parseBoolean(prop.getProperty("testingviewonly"));
			activeinterval = (Long.parseLong(prop.getProperty("activeinterval")))*1000L;
			replychance = Float.parseFloat(prop.getProperty("replychance"));
			smart = Boolean.parseBoolean(prop.getProperty("smart"));
			//YCSB
			long initload = Long.parseLong(prop.getProperty("initload"));
			long initrun = Long.parseLong(prop.getProperty("initrun"));
			//CHURN
			String churn_type = prop.getProperty("churn_type");
			long start_time = Long.parseLong(prop.getProperty("start_time"));
			long stop_time = Long.parseLong(prop.getProperty("stop_time"));
			long churn_interval = Long.parseLong(prop.getProperty("churn_interval"));
			String churn_class = prop.getProperty("churn_class");
			double churn_percentage = Double.parseDouble(prop.getProperty("churn_percentage"));
			int nchurn = (int) Math.ceil(number_of_peers*churn_percentage);
			//CHURN PER GROUP - distributing churn for the groups
			int desiredngroups = 1;
			// ---- figuring out which is the desired ngroups 
			while(((number_of_peers/desiredngroups)>repmax)){
				desiredngroups = desiredngroups * 2;
			}
			int nchurnpergroup = (int) Math.ceil((number_of_peers/desiredngroups)*churn_percentage);
			
			
			
			//PRINTING INFO
			System.out.println("times -> bootime: "+boottime+" ycsbLoad: "+initload+" ycsbRun: "+initrun);
			if(churn_type.equals("off")){
				System.out.println("churn -> type: "+churn_type);
			}
			else{
				System.out.println("churn -> type: "+churn_type+" class: "+churn_class+" start: "+start_time+" stop: "+stop_time+" interval: "+churn_interval+" percentage: "+churn_percentage);
				System.out.println("churn numbers -> nchurn: "+nchurn+" desiredgroups: " +desiredngroups+" churnpergroup: "+nchurnpergroup);
			}
			System.out.println("Loading from file -> "+loadfromfile);
			
			if(initload!=0){
				initload = initload + boottime;
			}
			if(initrun!=0){
				initrun = initrun + boottime;
			}
			
			
			// Bootstrapper
			Host bootstrapperHost = world.createHost();
			Process bootproc = bootstrapperHost.createProcess();
			Entry<Bootstrapper> booter = bootproc.createEntry(bootstrapper.Bootstrapper.class,bootstrapper.BootstrapperImpl.class.getName());
			firstip = bootstrapperHost.getAddress().getCanonicalHostName();
			boot = booter.call().initBootstrapper(0,viewsize,firstip);
			booter.queue().main(new String[0]);
			
			System.out.println("Bootstrapper created and queued.");

			Entry<Peer>[] e = world.createEntries(number_of_peers,core.Peer.class,core.PeerImpl.class.getName());

			System.out.println("Initial Entries created in Minha. Going to initialize each with a Peer.");

			//forcing uniformity
			double step = 1.0/number_of_peers;
			double start = 0;

			//Writing data for Fake Load Balancer
			String peerlist = "";
			
			for(int i=0; i<e.length; i++){
				String ip = e[i].getProcess().getHost().getAddress().getCanonicalHostName();
				Double npos = start;
				start = start + step;
				long pid = lastid+1L;
				lastid = pid;
				long pssboottime = boottime*1000L;
				Peer p;
				if(loadfromfile){
					//pssboottime = 0L;
					String[] storedata = readFileToListOfLines("datain/"+pid+"-store.txt");
					String[] groupdata = readFileToListOfLines("datain/"+pid+"-groups.txt");
					String[] pssdata = readFileToListOfLines("datain/"+pid+"-pss.txt");
					p = e[i].call().initPeerWithData(ip,pid,loadfromfile,firstip,psssleepinterval,
							pssboottime,viewsize,repmax,repmin,maxage,localmessage,localinterval,loglevel,
							testingviewonly,activeinterval,replychance,smart,storedata,groupdata,pssdata);
				}
				else{
					p = e[i].call().initPeer(ip,pid,npos,loadfromfile,firstip,psssleepinterval,
						pssboottime,viewsize,repmax,repmin,maxage,localmessage,localinterval,loglevel,
						testingviewonly,activeinterval,replychance,smart);
				}
				peers.put(ip, p);
				if (loglevel.equals("debug")) System.out.println("Got Peer "+p.getID());
				//Writing data for Fake Load Balancer
				peerlist = peerlist + p.getIP() + " " + Peer.port + " " + p.getID() + " " + p.getPOS() +" ";
				entrylist.put(ip, e[i]);
				boot.addIP(ip,pid,npos);
				
				//test purposes
				int ideal_group_for_peer = (int) Math.ceil((new Double(desiredngroups))*npos);
				if(ideal_group_for_peer == 0){
					ideal_group_for_peer = 1;
				}
				ArrayList<String> list = idealconfig.get(ideal_group_for_peer);
				if(list==null){
					list = new ArrayList<String>();
					list.add(ip);
					idealconfig.put(ideal_group_for_peer, list);
				}
				else{
					list.add(ip);
					idealconfig.put(ideal_group_for_peer, list);
				}
				System.out.print(".");
			}
			//Writing data for Fake Load Balancer
			PrintWriter out = new PrintWriter(peerlistfile);
			out.println(peerlist);
			out.close();
			
			
			System.out.println("ALL peers initialized.");
			
			
			// TIMEADVANCER
			Host timehost = world.createHost();
			Process timeproc = timehost.createProcess();
			Entry<TimeAdvancer> time = timeproc.createEntry(simulation.TimeAdvancer.class,simulation.TimeAdvancerImpl.class.getName());
			time.call().initTimeAdvancer();
			String[] timeargs = new String[2];
			timeargs[0] = new Long(timeinterval).toString();
			timeargs[1] = new Long(boottime).toString();
			time.queue().main(timeargs);


			String loadip="", runip="";
			//YCSB
			if(initload!=0){
				loadip = queueYCSBLoad(initload);
			}
			if(initrun!=0){
				runip = queueYCSBRun(initrun);
			}
			
			//Queuing MAIN for PEERS
			int count =0;
			for (Map.Entry<String, Entry<Peer>> val : entrylist.entrySet()){
				//queue peer main
				val.getValue().queue().main(new String[0]);
				count = count + 1;
			}
			System.out.println("Queued "+count+" peers.");
			
			//COPY PEERLIST file to YCSB node virtual storage
			if(!loadip.equals("")){
				Runtime.getRuntime().exec("cp peerlist.properties "+loadip);
			}
			if(!runip.equals("")){
				Runtime.getRuntime().exec("cp peerlist.properties "+runip);
			}

			long startuptime = 0;
			if(boottime!=0){
				startuptime = world.run(boottime,TimeUnit.SECONDS);
			}

			
			//Churn control variables
			boolean churnonetime_done = false;
			int constantcyclecount = 0;
			int constantchurncycle = 1;
			
			//random
			Random rnd = new Random();
			
			//aux
			long localtime = 0;
			boolean snapshotcreated = false;
			
			//Observation Cycles Code
			for(int i=0;i<=cycles;i++){
				
				long now = world.run(timeinterval,TimeUnit.SECONDS);
				if (loglevel.equals("debug"))System.out.println("Minha simulation time: "+now);
				
				now = now - startuptime;
				
				localtime = localtime + timeinterval;
				if (loglevel.equals("debug"))System.out.println("LocalTime: "+localtime);
				
				//Logging
				logKeysetAndPSS(now,true);
				
				//Snapshot
				if(snapshottime!=0 && snapshottime<=localtime && !snapshotcreated){
					
					for (String snap : peers.keySet()){
						Peer psnap = peers.get(snap);
						SnapshotWriter sw = new SnapshotWriter(psnap);
						sw.writeSnapshot();
					}
					snapshotcreated = true;
					System.out.println("Snapshot of all peers done.");
				}
				
				//Churn
				if(churn_type.equals("onetime") && !churnonetime_done){
					if(start_time<=localtime && stop_time>=localtime){
						System.out.println("One time churn of "+nchurn+" nodes. Class:"+churn_class);
						if(churn_class.equals("remove")){
							//Remove nchurn nodes
							for(int j=0;j<nchurn;j++){
								if (loglevel.equals("debug"))System.out.println("Removing node.");
								removePeer();
							}
						}
						else{
							if(churn_class.equals("add")){
								//Add nchurn
								for(int j=0;j<nchurn;j++){
									if (loglevel.equals("debug"))System.out.println("Adding node.");
									addPeer(rnd.nextDouble());
								}
							}
							
						}
						churnonetime_done = true;
					}
				}
				else{
					if(churn_type.equals("constant")){
						if(start_time<=localtime && stop_time>=localtime){
							if(constantcyclecount%churn_interval==0){
								//Remove and add nchurn nodes
								System.out.println("Constant churn cycle "+constantchurncycle);
								constantchurncycle = constantchurncycle +1;
								//ArrayList<Double> removedpositions = new ArrayList<Double>();
								//removing distributing per group
								removeAddPeersDistributed(nchurnpergroup,desiredngroups,rnd);
//								for(String rip : ipstorem){
//									double rpos =removePeer(rip);	
//									removedpositions.add(rpos);	
//								}
//								for(int j=0;j<nchurn;j++){
//									double apos = removedpositions.remove(0);
//									addPeer(apos);									
//								}
								
							}
							constantcyclecount = constantcyclecount + new Long(timeinterval).intValue();
						}
					}
				}
			}

		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (Throwable e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}



	}

	//AUX
	
	private static String[] readFileToListOfLines(String path){
		if (loglevel.equals("debug"))System.out.println("Going to read from file "+path);
		BufferedReader reader;
		try {
			FileReader file = new FileReader(path);
			if (loglevel.equals("debug"))System.out.println("After new FileReader");
			reader = new BufferedReader(file);
			if (loglevel.equals("debug"))System.out.println("After new BufferedReader");
			ArrayList<String> list = new ArrayList<String>();
			if (loglevel.equals("debug"))System.out.println("read from file "+path+" list init:"+list.size());
			String line = reader.readLine();
			while (line != null) {
			  list.add(line);
			  line = reader.readLine();
			}
			if (loglevel.equals("debug"))System.out.println("READ FROM FILE "+list.size()+" lines.");
			String[] res = list.toArray(new String[list.size()]);
			reader.close();
			if (loglevel.equals("debug"))System.out.println("returning FROM FILE "+res.length+" lines.");
			return res;
		} catch (FileNotFoundException e) {
			System.err.println("File not found! Error reading from file path: "+path);
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("IOException! Error reading from file path: "+path);
			e.printStackTrace();
		}
		return null;
	}
	
	
	//CHURN-----------------------------------------------------------------------------------------------
	
	private static String addPeer(double spos) throws ContainerException, IllegalArgumentException, SecurityException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException{
		
		lastid = lastid + 1L;
		Long id = lastid;
    	Double position = spos;//new Double(rnd.nextDouble());
    	
		Host newhost = world.createHost();
		Process newproc = newhost.createProcess();
		String ip = newhost.getAddress().getCanonicalHostName();
		
		Entry<Peer> newpeer = newproc.createEntry(core.Peer.class,core.PeerImpl.class.getName());
		Peer newpeerref = newpeer.call().initPeer(ip, id, position,false,firstip,psssleepinterval,
				new Random().nextInt(3000),viewsize,repmax,repmin,maxage,localmessage,localinterval,loglevel,
				testingviewonly,activeinterval,replychance,smart);
		
		entrylist.put(ip, newpeer);
		peers.put(ip, newpeerref);
		boot.addIP(ip, id, position);
		newpeer.queue().main(new String[0]);
		return ip;
		
	}
	
	private static double removePeer() {
		ArrayList<String> ips = new ArrayList<String>();
		for(String ipp : peers.keySet()){
			ips.add(ipp);
		}
		Collections.shuffle(ips);
		String iptoremove = ips.get(0);
		System.out.println("Going to remove "+iptoremove);
		Peer tor = peers.get(iptoremove);
		double torpos = tor.getPOS();
		tor.stopPeer();
		entrylist.remove(iptoremove);
		peers.remove(iptoremove);
		boot.removeIP(iptoremove);
		return torpos;
	}
	
	private static double removePeer(String iptoremove){
		System.out.println("Going to remove "+iptoremove);
		Peer tor = peers.get(iptoremove);
		double torpos = tor.getPOS();
		tor.stopPeer();
		entrylist.remove(iptoremove);
		peers.remove(iptoremove);
		boot.removeIP(iptoremove);
		return torpos;
	}
	
	private static void removeAddPeersDistributed(int ntorem,int desiredngroups, Random rnd) throws IllegalArgumentException, SecurityException, ContainerException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException{
		for(int i=1;i<=desiredngroups;i++){
			ArrayList<String> ipsingroup = idealconfig.get(i);
			ArrayList<String> ips = new ArrayList<String>();
			for(String ipp : ipsingroup){
				ips.add(ipp);
			}
			Collections.shuffle(ips);
			if(ipsingroup.size()>=ntorem){
				for(int j=0;j<ntorem;j++){
					String toremove = ips.remove(0);
					double rempos = removePeer(toremove);
					String added = addPeer(rempos);
					ipsingroup.remove(toremove);
					ipsingroup.add(added);
					idealconfig.put(i, ipsingroup);
					System.out.println("Removed "+toremove+" and Added "+added);
				}
			}
			
			
		}
	}
	
	//----------------------------------------------------------------------------------------------------
	
	
	private static void logKeysetAndPSS(long now, boolean keyset){
		FileWriter fstream;
		try {
			fstream = new FileWriter("logs/groups/"+(now/1000000000)+".txt");
			BufferedWriter fout = new BufferedWriter(fstream);
			HashMap<Long,Integer> storedKeys = new HashMap<Long,Integer>();
			
			HashMap<Long,String> toprint = new HashMap<Long,String>();
			
			for(Peer pi : peers.values()){
				String s = pi.getPSSLog(); 
				if(!s.equals("")){
					toprint.put(pi.getID(), s);
				}
			}
			
			List<Long> keys = new ArrayList<Long>(toprint.keySet());
			Collections.sort(keys);
			for(Long key : keys){
				fout.write(toprint.get(key));
				fout.write("\n");
			}
			fout.close();
			
			if(keyset){
				for(Peer pi : peers.values()){
					Long [] tmp = pi.getStoredKeys();
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
				fstream = new FileWriter("logs/keyset/"+(now/1000000000)+".txt");
				fout = new BufferedWriter(fstream);
				List<Long> longkeys = new ArrayList<Long>(storedKeys.keySet());
				Collections.sort(longkeys);
				for(Long chave :longkeys){
					fout.write(chave+" "+storedKeys.get(chave)+"\n");
				}
				fout.close();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	

	
	private static String queueYCSBRun(long runtime){
		String ycsbip = "";
		try{
			Host ycsbhost = world.createHost();
			Process ycsbproc = ycsbhost.createProcess();
			Entry<Main> ycsb = ycsbproc.createEntry();
			
			ycsbip = ycsbhost.getAddress().getCanonicalHostName();
			Runtime.getRuntime().exec("mkdir "+ycsbip);
			Runtime.getRuntime().exec("cp -r workloads/ "+ycsbip);
			if (loglevel.equals("debug"))System.out.println("Scheduling YCSB run at "+runtime+" s");
			ycsb.at(runtime,TimeUnit.SECONDS).queue().main("com.yahoo.ycsb.Client","-t","-s","-threads","1","-db","client.YCSBGlue","-p","exportfile=ycsbRUN.txt",
					"-p","stratus.ip="+ycsbip,"-p",
					"stratus.port=8000","-p", "stratus.id=ycsbRun","-P", "workloads/workloadb");
			
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
		return ycsbip;
	}
	
	private static String queueYCSBLoad(long initload){
		Host ycsbhost;
		String ycsbip = "";
		try {
			ycsbhost = world.createHost();
			Process ycsbproc = ycsbhost.createProcess();
			Entry<Main> ycsb = ycsbproc.createEntry();
			
			ycsbip = ycsbhost.getAddress().getCanonicalHostName();
			Runtime.getRuntime().exec("mkdir "+ycsbip);
			Runtime.getRuntime().exec("cp -r workloads/ "+ycsbip);
			if (loglevel.equals("debug"))System.out.println("Scheduling YCSB load at "+initload+" s");
			ycsb.at(initload,TimeUnit.SECONDS).queue().main("com.yahoo.ycsb.Client","-load","-s","-threads","1","-db","client.YCSBGlue","-p","exportfile=ycsbLOAD.txt",
					"-p","stratus.ip="+ycsbip,"-p",
					"stratus.port=9000","-p", "stratus.id=ycsbload","-P", "workloads/workloadb");
			
			
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
		return ycsbip;
	}
}
