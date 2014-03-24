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
