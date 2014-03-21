package ycsbglue;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;


import client.Client;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import common.PeerData;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import loadbalancing.FakeLoadBalancer;
import loadbalancing.LoadBalancer;


public class StratusClient extends DB {
	
	public static int lastport = 0;
	
	private LoadBalancer lb;
	private Client client;
	private long reqcount = 0;
	private Logger log;
	
	private long getReqId(){
		this.reqcount++;
		return this.reqcount;
	}
	
	public static int getNewPort(int p){
		synchronized(StratusClient.class){
			if(lastport==0){
				lastport=p;
			}
			lastport++;
			return lastport;
		}
	}

	
	public StratusClient(){
			
	}

	public void init() throws DBException{
		
		Properties ycsbProps = getProperties();
		
		String myIp = ycsbProps.getProperty("stratus.ip");
		int myPort = Integer.parseInt(ycsbProps.getProperty("stratus.port"));
		String myID = ycsbProps.getProperty("stratus.id");
		myPort = getNewPort(myPort);
		
		String myself = myIp+':'+new Integer(myPort).toString();
		
		log = Logger.getLogger(myself);
		//Configuring Log
		log.setLevel(Level.DEBUG);
		FileAppender capp = null;
		try {
			capp = new FileAppender(new PatternLayout("%d{HH:mm:ss,SSS} %r [%t] %-5p %c %x - %m%n"),"logs/"+myIp+myPort+myID+".txt");
		} catch (IOException e1) {
			
			e1.printStackTrace();
		}
		capp.setName(myself);
		log.addAppender(capp);
		
		log.debug("StratusClient STARTED IP:"+myIp+" PORT:"+myPort);
		ArrayList<PeerData> peers = new ArrayList<PeerData>();
		
		//File where all nodes are described - used for fake load balancer
		String peerlistfile = "peerlist.properties";

		BufferedReader f;
		try {
			f = new BufferedReader(new FileReader(peerlistfile));

			String peerslisting = f.readLine();
			log.debug(peerslisting);
			f.close();

			String[] plist = peerslisting.split(" ");
			int size = plist.length;
			for (int i=0;i<size;i=i+4){
				String ip = plist[i];
				//int port = Integer.parseInt(plist[i+1]);
				Long pid = Long.parseLong(plist[i+2]);
				
				PeerData tmp = new PeerData(ip,0,0,0,0,pid);
				peers.add(tmp);
				log.debug("READPEER - "+pid + " PEERSSIZE "+peers.size());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Starting the peer sampling service
		lb = new FakeLoadBalancer(peers,log, new Random());
		//For now the number of replies needed is one and it is hardcoded
		int nputreplies = 1;
		//The Client will always have the id 0 - req id is distinguished by PORT
		client = new Client(new Long(0).toString(),lb,myIp,myPort,nputreplies,log);
		log.debug("StratusClient started.");
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
		long key = this.hash(arg0+arg1);
		String[] keyset = arg2.keySet().toArray(new String[1]);
		String column = keyset[0];
		ByteIterator vl = arg2.get(column);
		String value = column+";"+new String(vl.toArray());
		log.debug("YCSB put request.");
		this.client.put(key, value.getBytes());
		arg2.get("");
		return 0;
	}

	@Override
	public int read(String arg0, String arg1, Set<String> arg2, HashMap<String, ByteIterator> arg3) {
		long key = this.hash(arg0+arg1);
		log.debug("YCSB read request.");
		byte[] res = this.client.get(this.getReqId(), key);
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
