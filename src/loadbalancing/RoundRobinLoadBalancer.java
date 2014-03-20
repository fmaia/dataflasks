package loadbalancing;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import common.PeerData;

public class RoundRobinLoadBalancer implements LoadBalancer {

	private ArrayList<PeerData> allnodes;
	private Logger log;
	private int pos;

	public RoundRobinLoadBalancer(ArrayList<PeerData> total,Logger log){
		this.allnodes = total;
		this.log = log;
		this.log.debug("RoundRobinLoadBalancer initialized.");
		this.pos = -1;
	}
	
	@Override
	public PeerData getRandomPeer() {
		this.pos = this.pos + 1;
		if(this.pos == this.allnodes.size()){
			this.pos = 0;
		}
		return allnodes.get(this.pos);
	}

}
