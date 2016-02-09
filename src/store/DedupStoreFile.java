package store;

import java.util.ArrayList;

public class DedupStoreFile {
	
	public ArrayList<String> listofblocks;
	
	public int len;
	
	public DedupStoreFile(){
		this.listofblocks = new ArrayList<String>();
		this.len = 0;
	}
	
}
