package loadbalancing;

import java.util.ArrayList;
import java.util.Random;

import org.apache.log4j.Logger;

import common.PeerData;

public class FakeLoadBalancer implements LoadBalancer {

	private ArrayList<PeerData> allnodes;
	private Logger log;
	private Random rnd;

	public FakeLoadBalancer(ArrayList<PeerData> total,Logger log, Random rnd){
		this.allnodes = total;
		this.log = log;
		this.log.debug("FakeLoadBalancer initialized.");
		this.rnd = rnd;
	}
	
	@Override
	public synchronized PeerData getRandomPeer(){
		int listSize = this.allnodes.size();
		int randi = rnd.nextInt(listSize);
		PeerData tmp = this.allnodes.get(randi);
		return tmp;
	}

}
