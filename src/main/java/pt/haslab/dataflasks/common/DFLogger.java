package pt.haslab.dataflasks.common;

import java.io.IOException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

public class DFLogger {
	private String name;
	private org.apache.log4j.Logger log;
	public DFLogger(String name, String loglevel){
		this.log = org.apache.log4j.Logger.getLogger(name);
		this.log.setAdditivity(false);
		if(loglevel.equals("info")){
			this.log.setLevel(Level.INFO);
		}
		else{
			if(loglevel.equals("off")){
				this.log.setLevel(Level.OFF);
			}
			else{
				this.log.setLevel(Level.DEBUG);
			}
		}
		FileAppender capp = null;
		try {
			capp = new FileAppender(new PatternLayout("[%d] - %m%n"),"logs/"+name+".txt");
		} catch (IOException e1) {

			e1.printStackTrace();
		}
		
		capp.setName(name);
		this.log.removeAllAppenders();
		this.log.addAppender(capp);
		this.name = name;
	}
	
	

	public  void info(String message){
		String m = "["+this.name+"] INFO "+message;
		this.log.info(m);
	}
	
	public  void debug(String message){
		String m = "["+this.name+"] DEBUG "+message;
		this.log.debug(m);
	}
	
	public  void warn(String message){
		String m = "["+this.name+"] WARN "+message;
		this.log.warn(m);
	}
	
	public  void error(String message){
		String m = "["+this.name+"] ERROR "+message;
		this.log.error(m);
	}
}
