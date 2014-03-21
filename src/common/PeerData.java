package common;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class PeerData implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String ip;
	private int nslices;
	private int age;
	private int slice;
	private double pos;
	private long myID;
	
	
	public PeerData(String ip, int nslices,int age,int slice, double pos, long myID){
		this.ip = ip;
		this.nslices = nslices;
		this.age = age;
		this.slice = slice;
		this.pos = pos;
		this.myID = myID;
	}
	
	public PeerData(){
		
	}
	

	public void writeObject(ObjectOutputStream s){
		try {
			s.writeObject(ip);
			s.writeInt(nslices);
			s.writeInt(age);
			s.writeInt(slice);
			s.writeDouble(pos);
			s.writeLong(myID);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void readObject(ObjectInputStream s){
		try {
			this.ip = (String) s.readObject();
			this.nslices = s.readInt();
			this.age = s.readInt();
			this.slice = s.readInt();
			this.pos = s.readDouble();
			this.myID = s.readLong();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public double getPos() {
		return pos;
	}

	public void setPos(double pos) {
		this.pos = pos;
	}

	public long getID() {
		return myID;
	}

	public void setID(long myID) {
		this.myID = myID;
	}

	public int getNslices() {
		return nslices;
	}

	public void setNslices(int nslices) {
		this.nslices = nslices;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public int getSlice() {
		return slice;
	}

	public void setSlice(int slice) {
		this.slice = slice;
	}

	public long getMyID() {
		return myID;
	}

	public void setMyID(long myID) {
		this.myID = myID;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public void age(){
		this.age = this.age +1;
	}
	

	
	@Override
	public boolean equals(Object o){
		boolean res = false;
		if (this==o){
			res = true;
		}
		else{
			if(o instanceof PeerData){
				PeerData tmp = (PeerData) o;
				res =  (tmp.getID() == this.getID());
			}
		}
		return res;
	}
	
	@Override
	public Object clone(){
		PeerData tmp = new PeerData(new String(this.ip),this.nslices,this.age,this.slice,this.pos,this.myID);
		return tmp;
	}
	
	@Override
	public String toString(){
		return new Long(this.myID).toString();
	}
}
