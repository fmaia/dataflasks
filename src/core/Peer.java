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
package core;


import pt.minha.api.sim.Global;

@Global
public interface Peer {

	public static int port = 6001;
	public static int pssport = 6002;
	public static int bootport = 6003;
	
	public void main(String[] args);
	public Peer initPeer(String ip,long id,double position,boolean loadfromfile,String bootip, 
			long psssleepinterval, long boottime, int pssviewsize, int repmax, 
			int repmin, int maxage, boolean localmessage, int localinterval,String loglevel,
			boolean testingviewonly,long activeinterval,float replychance,boolean smart);
	public void snapshot();
	public long getID();
	public String getIP();
	public double getPOS();
	public void stopPeer();
	public String getPSSLog();
	public Long[] getStoredKeys();
}
