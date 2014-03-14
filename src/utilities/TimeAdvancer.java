package utilities;

import pt.minha.api.sim.Global;


@Global
public interface TimeAdvancer {

	public void main(String[] args);
	public void stop();
	public TimeAdvancer initTimeAdvancer();

}