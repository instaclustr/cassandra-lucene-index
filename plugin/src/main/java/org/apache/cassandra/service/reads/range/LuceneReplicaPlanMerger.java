package org.apache.cassandra.service.reads.range;

import java.util.Iterator;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.ReplicaPlan;

public class LuceneReplicaPlanMerger extends ReplicaPlanMerger {
    public LuceneReplicaPlanMerger(final Iterator<ReplicaPlan.ForRangeRead> iterator,
                                   final Keyspace keyspace,
                                   final ConsistencyLevel consistency) {
        super(iterator, keyspace, consistency);
    }
}
