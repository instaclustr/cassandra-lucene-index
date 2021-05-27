package org.apache.cassandra.service.reads.range;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;

public class LuceneReplicaPlanIterator extends ReplicaPlanIterator {

    public LuceneReplicaPlanIterator(final AbstractBounds<PartitionPosition> keyRange,
                                     final Keyspace keyspace,
                                     final ConsistencyLevel consistency) {
        super(keyRange, keyspace, consistency);
    }
}
