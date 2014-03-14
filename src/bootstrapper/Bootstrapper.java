package bootstrapper;

import pt.minha.api.sim.Global;

@Global
public interface Bootstrapper {

	public void main(String[] args);

	public Bootstrapper initBootstrapper(long id,int viewsize,String ip);
	public void addIP(String ip, long id, double pos);
	
	public void removeIP(String ip);
	
}
