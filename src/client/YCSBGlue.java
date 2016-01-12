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
package client;


import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import core.Peer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import common.DFLogger;

import loadbalancing.DynamicLoadBalancer;
import loadbalancing.LBPassiveThread;
import loadbalancing.LoadBalancer;
//import loadbalancing.RandomLoadBalancer;


public class YCSBGlue extends DB {
	
	public static int lastport = 0;
	public static int lastsenderport = 0;
	
	private LoadBalancer lb;
	private Client client;
	private long reqcount = 0;
	private DFLogger log;
	
	private long getReqId(){
		this.reqcount++;
		return this.reqcount;
	}
	
	public static int getNewPort(int p){
		synchronized(YCSBGlue.class){
			if(lastport==0){
				lastport=p;
			}
			lastport++;
			return lastport;
		}
	}

	public static int getNewSenderPort(int p){
		synchronized(YCSBGlue.class){
			if(lastsenderport==0){
				lastsenderport=p;
			}
			lastsenderport++;
			return lastsenderport;
		}
	}
	
	public YCSBGlue(){
			
	}

	public void init() throws DBException{
		
		
		
		Properties ycsbProps = getProperties();
		
		String myIp = ycsbProps.getProperty("stratus.ip");
		int myPort = Integer.parseInt(ycsbProps.getProperty("stratus.port"));
		//String myID = ycsbProps.getProperty("stratus.id");
		Long waittimeout = Long.parseLong(ycsbProps.getProperty("stratus.timeout"));
		Long lbinterval = Long.parseLong(ycsbProps.getProperty("stratus.lbinterval"));
		String bootip = ycsbProps.getProperty("stratus.bootip");
		myPort = getNewPort(myPort);
		
		//String myself = myIp+':'+new Integer(myPort).toString();
		
		log = new DFLogger("ycsbglue."+myIp);
		
		log.debug("YCSBGlue STARTED IP:"+myIp+" PORT:"+myPort);
		
		//Starting load balancer
		//lb = new RandomLoadBalancer(log,new Random());
		lb = new DynamicLoadBalancer(log, new Random(),bootip,myIp,lbinterval);
		new Thread(new LBPassiveThread((DynamicLoadBalancer) lb,myIp,log)).start();
		new Thread((Runnable) lb).start();
		
		//For now the number of replies needed is one and it is hardcoded
		int nputreplies = 1;
		//The Client will always have the id 0 - req id is distinguished by PORT
		int senderport = getNewSenderPort(Peer.outclientport);
		System.out.println("Initializing YCSBGlue. Port:"+myPort+" senderPort:"+senderport);
		client = new Client(new Long(0).toString(),lb,myIp,myPort,senderport,nputreplies,waittimeout,log);
		log.debug("YCSBGlue started.");
	}
	
	/**
	 * @return most significant 8 bytes of the MD5 hash of the string, as a long
	 */
	private long hash(String value) {
		byte[] md5hash;
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		md5hash = md.digest(value.getBytes());
		long result = ByteBuffer.wrap(md5hash).getLong();
		return result;
	}
	
	@Override
	public int delete(String arg0, String arg1) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int insert(String arg0, String arg1, HashMap<String, ByteIterator> arg2) {
		//FIX ME - abs function below is for test purposes only. Linked with fix me from kvstore sliceforkey method
		//FIX ME - version is being ignored!
		long key = Math.abs(this.hash(arg0+arg1));
		String[] keyset = arg2.keySet().toArray(new String[1]);
		String column = keyset[0];
		ByteIterator vl = arg2.get(column);
		String value = column+";"+new String(vl.toArray());
		log.debug("YCSB put request.");
		Set<Long> res = null;
		while(res==null){
			log.debug("Issuing put operation...");
			res = this.client.put(key,key, value.getBytes());
			if(res==null){
				log.debug("put operation failed... retrying");
			}
		}
		arg2.get("");
		return 0;
	}

	@Override
	public int read(String arg0, String arg1, Set<String> arg2, HashMap<String, ByteIterator> arg3) {
		//FIX ME - abs function below is for test purposes only. Linked with fix me from kvstore sliceforkey method
		//FIX ME - version is being ignored!
		long key = Math.abs(this.hash(arg0+arg1));
		log.debug("YCSB read request.");
		byte[] res =null;
		while(res==null){
			log.debug("Issuing get operation...");
			res = this.client.get(this.getReqId(), key,key);
			if(res==null){
				log.debug("get operation failed... retrying");
			}
		}
		String vl = new String(res);
		String[] vls = vl.split(";");
		ByteIterator value = new ByteArrayByteIterator(vls[1].getBytes());
		String column = vls[0];
		arg3.put(column, value);
		return 0;
	}

	@Override
	public int scan(String arg0, String arg1, int arg2, Set<String> arg3,
			Vector<HashMap<String, ByteIterator>> arg4) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int update(String arg0, String arg1,
			HashMap<String, ByteIterator> arg2) {
		// TODO Auto-generated method stub
		return 0;
	}

}
