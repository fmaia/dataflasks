package simulation;

import java.io.FileNotFoundException;
import java.io.PrintWriter;


import core.Peer;


public class SnapshotWriter {

	private String store;
	private String cyclon;
	private String flasks;
	private String path;
	
	public SnapshotWriter(Peer p){
		this.store = p.getStoreSnap();
		this.cyclon = p.getPSSSnap();
		this.flasks = p.getGroupConstructionSnap();
		this.path = "dataout/"+p.getID()+"-";
	}
	
	
	public void writeSnapshot(){
		
		PrintWriter pw;
		try {
			pw = new PrintWriter(path+"store.txt");
			pw.write(this.store);
			pw.flush();
			pw.close();
			pw = new PrintWriter(path+"pss.txt");
			pw.write(this.cyclon);
			pw.flush();
			pw.close();
			pw = new PrintWriter(path+"groups.txt");
			pw.write(this.flasks);
			pw.flush();
			pw.close();
			//System.out.println("Snapshot of data created in "+path);
		} catch (FileNotFoundException e) {
			System.err.println("Error in snapshot creation: "+path);
			e.printStackTrace();
		}
		
	}
}
