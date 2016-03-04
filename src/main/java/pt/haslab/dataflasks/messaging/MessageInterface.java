package pt.haslab.dataflasks.messaging;

public interface MessageInterface {
	
	public void decodeMessage(byte[] packet);
	public byte[] encodeMessage();
	public long getMessageKey();
	public String getMessageIP();
	public long getMessageID();
	public int getMessagePort();
	public MessageType getMessageType();
}
