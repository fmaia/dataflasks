package loadbalancing;

import common.PeerData;

public interface LoadBalancer {
	
	public PeerData getRandomPeer();
}
