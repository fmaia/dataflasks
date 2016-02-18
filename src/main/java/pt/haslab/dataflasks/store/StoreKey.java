package pt.haslab.dataflasks.store;

import java.lang.ClassCastException;

import pt.minha.api.sim.Global;

@Global
public class StoreKey implements Comparable<Object>{

	public long key;
	public long version;
	
	public StoreKey(long k, long v){
		this.key = k;
		this.version = v;
	}
	
	@Override
	public String toString(){
		String res = "(" + this.key + "," + this.version + ")";
		return res;
	}
	
	@Override
	public boolean equals(Object other){
		if(other==this){
			return true;
		}
		else{
			if(!(other instanceof StoreKey)){
				return false;
			}
			else{
				return ((this.key==((StoreKey) other).key) && (this.version==((StoreKey) other).version));
			}
		}
	}
	
	@Override 
	public int hashCode()
	{
		//This hasCode may not be very efficient due to the possibility of collisions 
		return (int) (key+version);
	}


	
	public int compareTo(Object o) {
		if(o==this){
			return 0;
		}
		else{
			if(!(o instanceof StoreKey)){
				ClassCastException e = new ClassCastException();
				throw e;
			}
			else{
				StoreKey other = (StoreKey) o;
				if(other.key==this.key){
					if(other.version >this.version){
						return -1;
					}
					else{
						if(other.version < this.version){
							return 1;
						}
						else{
							return 0;
						}
					}
				}
				else{
					if(other.key>this.key){
						return -1;
					}
					else{
						return 1;
					}
				}
			}
		}
	}
}
