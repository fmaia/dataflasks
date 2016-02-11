package store;


import java.util.ArrayList;

import pt.minha.api.sim.Global;

@Global
public class RabinExtractor {


    public native String[] extractblocks(int size, byte []data);

  
    public ArrayList<RabinBlock> getblocks(int size, byte[] data){
    	ArrayList<RabinBlock> blocks=new ArrayList<RabinBlock>();
        System.loadLibrary("rabinfingerprints");
        //System.out.println("library loaded");
        String[] res = this.extractblocks(size,data);
        int pos=0;
        for( String el : res ){
        	if(!el.equals("")){
        		//BE VERY CAREFULL the number 7 is hardcoded due to the rabingenerator.c
            	int ressize = new Integer(el.substring(el.length()-7));
                byte[] resdata = new byte[ressize];
                //System.arraycopy(data, pos, resdata, 0, size);
                for (int i=0;i<ressize;i++){
                	resdata[i]=data[pos+i];
                }
                blocks.add(new RabinBlock(el,ressize,resdata));
                pos = pos+ressize;
            	//System.out.println(el);
        	}	
        }
        //System.out.println("get blocks processed");
        return blocks;
    }



}