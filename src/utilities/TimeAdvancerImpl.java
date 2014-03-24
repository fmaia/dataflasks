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
