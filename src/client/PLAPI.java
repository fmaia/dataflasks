package client;

import java.util.Set;


public interface PLAPI {

    public Set<Long> put (long key,byte[] data);
    //returns a set of NodeIDs that stored the object.

    
    //public boolean set(long nodeID,long key,byte[] data);
    //set -> update; nodeID is an ID of a node the upper layer thinks has the object.
    //TODO: To be added.
    
    
    public byte[] get(long nodeID,long key);

    public byte[] delete(long nodeID,long key);

}
