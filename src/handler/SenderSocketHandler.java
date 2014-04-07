package handler;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import core.Peer;


public class SenderSocketHandler {

	private BlockingQueue<DatagramSocket> available;
	
	public SenderSocketHandler(int n){
		this.available = new LinkedBlockingQueue<DatagramSocket>(n);
		int senderport = Peer.outport;
		for(int i=0;i<n;i++){
			try {
				DatagramSocket socket = new DatagramSocket(senderport);
				available.put(socket);
			} catch (SocketException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
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
