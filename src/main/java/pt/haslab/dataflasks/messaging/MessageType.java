package pt.haslab.dataflasks.messaging;

public enum MessageType {
	GET (3), 
	PUT (2), 
	REPLICAMAINTENANCE (4), 
	STOP (5), 
	GETREPLY (10),
	PUTREPLY (11),
	MISSINGHASHREQ (50),
	MISSINGHASHREPLY (51);
	private final int serial;
	
	MessageType(int serial){
		this.serial = serial;
	}
	
	public int serial(){
		return this.serial;
	}
	
	public static MessageType getType(int v){
		switch(v){
		case 2:
			return PUT;
		case 3:
			return GET;
		case 4:
			return REPLICAMAINTENANCE;
		case 10:
			return GETREPLY;
		case 11:
			return PUTREPLY;
		case 50:
			return MISSINGHASHREQ;
		case 51:
			return MISSINGHASHREPLY;
		default:
			return STOP;
		}
	}
	
	public static int getValueType(MessageType a){
		switch(a){
		case PUT:
			return 2;
		case GET:
			return 3;
		case REPLICAMAINTENANCE:
			return 4;
		case GETREPLY:
			return 10;
		case PUTREPLY:
			return 11;
		case MISSINGHASHREQ:
			return 50;
		case MISSINGHASHREPLY:
			return 51;
		default:
			return 5;
		}
	}
}

