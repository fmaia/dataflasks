package pt.haslab.dataflasks.handler;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import pt.haslab.dataflasks.common.DFLogger;

import pt.haslab.dataflasks.client.YCSBGlue;
import pt.haslab.dataflasks.core.Peer;


public class SenderSocketHandler {

	private BlockingQueue<DatagramSocket> available;
	private static int shlastport = 0;
	
	public static int getNewSenderPort(int initport){
		int res = 0;
		synchronized(YCSBGlue.class){
			if(shlastport==0){
				shlastport=initport;
			}
			shlastport++;
			res = shlastport;
		}
		return res;
	}
	
	public SenderSocketHandler(int n, DFLogger log){
		log.info("before creating queue");
		this.available = new LinkedBlockingQueue<DatagramSocket>(n);
		for(int i=0;i<n;i++){
			try {
				log.info("before asking port");
				int senderport = getNewSenderPort(Peer.outport);
				log.info("before creating socket");
				DatagramSocket socket = new DatagramSocket(senderport);
				available.put(socket);
			} catch (SocketException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		log.info("Sender Socket initialized");
	}
	
	public DatagramSocket getSocket(){
		DatagramSocket res = null;
		try {
			 res = this.available.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return res;
	}
	
	public void returnSocket(DatagramSocket ds){
		try {
			this.available.put(ds);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
