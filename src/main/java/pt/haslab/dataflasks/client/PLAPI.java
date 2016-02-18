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
package pt.haslab.dataflasks.client;

import java.util.Set;


public interface PLAPI {

    public Set<Long> put (long key,long version, byte[] data);
    //returns a set of NodeIDs that stored the object.

    
    //public boolean set(long nodeID,long key,byte[] data);
    //set -> update; nodeID is an ID of a node the upper layer thinks has the object.
    //TODO: To be added.
    
    
    public byte[] get(long nodeID,long version, long key);

    public byte[] delete(long nodeID, long version, long key);

}
