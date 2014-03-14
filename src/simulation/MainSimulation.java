package simulation;

import java.io.PrintWriter;

import pt.minha.api.Entry;
import pt.minha.api.Host;
import pt.minha.api.World;
import pt.minha.api.sim.Simulation;
import pt.minha.api.Process;
import utilities.TimeAdvancer;

import java.util.HashMap;
import java.util.Map;
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

			int viewsize = 10;
			int number_of_peers = 50;
			boolean loadfromfile = false;
			long timeinterval = 1L;
			long boottime = 0L;
			String peerlistfile = "peerlist.properties";
			int cycles = 50;

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
				e[i].queue().initPeer(ip,lastid+1,npos,loadfromfile,firstip);
				entrylist.put(ip, e[i]);

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
				boot.addIP(p.getIP(),p.getID(),p.getPOS());
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

}
