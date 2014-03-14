package core;

import pt.minha.api.sim.Global;

@Global
public interface Peer {

	public static int port = 6001;
	public static int pssport = 6002;
	public static int bootport = 6003;
	
	public void main(String[] args);
	public Peer initPeer(String ip,long id,double position,boolean loadfromfile,String bootip);
	
	public Long[] getStoredKeys(long now);
	public void snapshot();
	public long getID();
	public String getIP();
	public double getPOS();
	public void stopPeer();
	public String getPSSLog();
}
