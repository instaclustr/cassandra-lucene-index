/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.stratio.cassandra.lucene.Index;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.range.LuceneReplicaPlanIterator;
import org.apache.cassandra.service.reads.range.LuceneReplicaPlanMerger;
//import org.apache.cassandra.service.StorageProxy.RangeIterator;
//import org.apache.cassandra.service.StorageProxy.RangeMerger;

/**
 * Modified version of Apache Cassandra {@link StorageProxy} to be used with Lucene searches.
 */
public class LuceneStorageProxy {

    private static final ClientRequestMetrics readMetrics = new ClientRequestMetrics("Read");

    private static Method systemKeyspaceQuery;
    private static Method fetchRows;

    static {
        try {
            systemKeyspaceQuery = StorageProxy.class.getDeclaredMethod("systemKeyspaceQuery", List.class);
            systemKeyspaceQuery.setAccessible(true);
            fetchRows = StorageProxy.class.getDeclaredMethod("fetchRows", List.class, ConsistencyLevel.class, long.class);
            fetchRows.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean systemKeyspaceQuery(List<? extends ReadCommand> cmds) throws ReflectiveOperationException {
        return (boolean) systemKeyspaceQuery.invoke(null, cmds);
    }

    private static PartitionIterator fetchRows(List<SinglePartitionReadCommand> commands, ConsistencyLevel cl, long queryStartNanoTime)
    throws ReflectiveOperationException {
        return (PartitionIterator) fetchRows.invoke(null, commands, cl, queryStartNanoTime);
    }

    public static PartitionIterator read(SinglePartitionReadCommand.Group group,
                                         ConsistencyLevel consistencyLevel,
                                         long queryStartNanoTime)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException,
           InvalidRequestException, ReflectiveOperationException {

        if (StorageService.instance.isBootstrapMode() && !systemKeyspaceQuery(group.queries)) {
            readMetrics.unavailables.mark();
            throw new IsBootstrappingException();
        }

        return readRegular(group, consistencyLevel, queryStartNanoTime);
    }

    private static PartitionIterator readRegular(SinglePartitionReadCommand.Group group,
                                                 ConsistencyLevel consistencyLevel,
                                                 long queryStartNanoTime)
    throws UnavailableException, ReadFailureException, ReadTimeoutException, ReflectiveOperationException {
        long start = System.nanoTime();
        try {
            PartitionIterator result = fetchRows(group.queries, consistencyLevel, queryStartNanoTime);
            // If we have more than one command, then despite each read command honoring the limit, the total result
            // might not honor it and so we should enforce it
            if (group.queries.size() > 1) {
                ReadCommand command = group.queries.get(0);
                TableMetadata metadata = group.metadata();
                ColumnFamilyStore cfs = Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.name);
                Index index = (Index) command.getIndex(cfs);
                result = index.postProcessorFor(group).apply(result, group);
                result = group.limits().filter(result, group.nowInSec(),true, metadata.enforceStrictLiveness());
            }

            return result;

        } catch (UnavailableException e) {
            readMetrics.unavailables.mark();
            throw e;
        } catch (ReadTimeoutException e) {
            readMetrics.timeouts.mark();
            throw e;
        } catch (ReadFailureException e) {
            readMetrics.failures.mark();
            throw e;
        } finally {
            long latency = System.nanoTime() - start;
            readMetrics.addNano(latency);
            // TODO avoid giving every command the same latency number.  Can fix this in CASSANDRA-5329
            for (ReadCommand command : group.queries) {
                Keyspace.openAndGetStore(command.metadata()).metric.coordinatorReadLatency.update(latency,
                                                                                                  TimeUnit.NANOSECONDS);
            }
        }
    }

    public static LuceneReplicaPlanMerger rangeMerger(PartitionRangeReadCommand command, ConsistencyLevel consistency)
    {
        final Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        final LuceneReplicaPlanIterator replicaPlans = new LuceneReplicaPlanIterator(command.dataRange().keyRange(), keyspace, consistency);
        return new LuceneReplicaPlanMerger(replicaPlans, keyspace, consistency);
    }
}
