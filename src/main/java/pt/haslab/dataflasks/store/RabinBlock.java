package pt.haslab.dataflasks.store;

import java.util.Arrays;

import pt.minha.api.sim.Global;

@Global
public class RabinBlock {

        public String hash;
        public byte [] data;
	public int size;	

        public RabinBlock(String hash, int size, byte[] data){
                this.hash=hash;
                this.data = Arrays.copyOf(data,data.length);
		this.size=size;

        }
}
