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
package pt.haslab.dataflasks.loadbalancing;

import java.util.ArrayList;

import pt.haslab.dataflasks.common.DFLogger;

import pt.haslab.dataflasks.common.PeerData;

public class RoundRobinLoadBalancer implements LoadBalancer {

	private ArrayList<PeerData> allnodes;
	private DFLogger log;
	private int pos;

	public RoundRobinLoadBalancer(ArrayList<PeerData> total,DFLogger log){
		this.allnodes = total;
		this.log = log;
		this.log.debug("RoundRobinLoadBalancer initialized.");
		this.pos = -1;
	}
	

	public PeerData getRandomPeer() {
		this.pos = this.pos + 1;
		if(this.pos == this.allnodes.size()){
			this.pos = 0;
		}
		return allnodes.get(this.pos);
	}

}
