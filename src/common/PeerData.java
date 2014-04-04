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
package common;


public class PeerData{
	
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
	
	public PeerData(String s){
		String[] values = s.split(" ");
		this.ip = values[0];
		this.nslices = Integer.parseInt(values[1]);
		this.age = Integer.parseInt(values[2]);
		this.slice = Integer.parseInt(values[3]);
		this.pos = Double.parseDouble(values[4]);
		this.myID = Long.parseLong(values[5]);
	}
	

	@Override
	public String toString(){
		String res = this.ip + " " + this.nslices+ " "+ this.age+" "+this.slice;
		res = res + " " + this.pos + " " +this.myID;
		return res;
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
	

}
