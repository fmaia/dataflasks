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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import pt.haslab.dataflasks.common.DFLogger;

import pt.haslab.dataflasks.common.PeerData;

public class RandomLoadBalancer implements LoadBalancer {

	private ArrayList<PeerData> allnodes;
	private DFLogger log;
	private Random rnd;

	public RandomLoadBalancer(DFLogger log, Random rnd){
		this.allnodes= new ArrayList<PeerData>();
		
		//File where all nodes are described - used for fake load balancer
		String peerlistfile = "peerlist.properties";
		log.debug("Going to read peerlist.");
		BufferedReader f;
		try {
			f = new BufferedReader(new FileReader(peerlistfile));
			log.debug("Going to read peerlist - readline");
			String peerslisting = f.readLine();
			log.debug("Peerlist read:"+peerslisting);
			f.close();

			String[] plist = peerslisting.split(" ");
			int size = plist.length;
			for (int i=0;i<size;i=i+4){
				String ip = plist[i];
				//int port = Integer.parseInt(plist[i+1]);
				Long pid = Long.parseLong(plist[i+2]);
				double position = Double.parseDouble(plist[i+3]);
				PeerData tmp = new PeerData(ip,0,0,0,position,pid);
				allnodes.add(tmp);
				//log.debug("READPEER - "+pid + " PEERSSIZE "+peers.size());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.log = log;
		this.log.debug("FakeLoadBalancer initialized.");
		this.rnd = rnd;
	}
	

	public synchronized PeerData getRandomPeer(){
		int listSize = this.allnodes.size();
		int randi = rnd.nextInt(listSize);
		PeerData tmp = this.allnodes.get(randi);
		return tmp;
	}

}
