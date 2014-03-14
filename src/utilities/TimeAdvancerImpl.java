package utilities;

public class TimeAdvancerImpl implements TimeAdvancer {

	/**
	 * @param args
	 */
	
	
	public TimeAdvancerImpl(){
	}
	
	private boolean running = true;
	
	/* (non-Javadoc)
	 * @see entry.TimeAdvancerInterface#stop()
	 */
	@Override
	public void stop(){
		this.running = false;
	}

	@Override
	public TimeAdvancer initTimeAdvancer() {
		return this;
	}
	
	/* (non-Javadoc)
	 * @see entry.TimeAdvancerInterface#main(java.lang.String[])
	 */
	@Override
	public void main(String[] args) {
		long time = 0;
		long interval = Long.parseLong(args[0]);
		long boottime = Long.parseLong(args[1]);
		time = time - boottime;
		System.out.println("Time: "+time);
		while(this.running){
			try {
				Thread.sleep(interval*1000);
				time = time + interval;
				System.out.println("Time: "+time);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		

	}


}
