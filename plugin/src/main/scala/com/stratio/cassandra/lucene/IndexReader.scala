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
package com.stratio.cassandra.lucene

import com.stratio.cassandra.lucene.index.DocumentIterator
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.ClusteringIndexFilter
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator
import org.apache.cassandra.db.rows.UnfilteredRowIterator
import org.apache.cassandra.schema.TableMetadata

/** [[UnfilteredPartitionIterator]] for retrieving rows from a [[DocumentIterator]].
  *
  * @param command    the read command
  * @param table      the base table
  * @param controller the read execution controller
  * @param documents  the documents iterator
  * @author Andres de la Pena `adelapena@stratio.com`
  */
abstract class IndexReader(
    command: ReadCommand,
    table: ColumnFamilyStore,
    controller: ReadExecutionController,
    documents: DocumentIterator)
  extends UnfilteredPartitionIterator {

  private lazy val metadataVal: TableMetadata = table.metadata.get()

  protected var nextData: Option[UnfilteredRowIterator] = None

  /** @inheritdoc */
  override def metadata: TableMetadata = {
    metadataVal
  }

  /** @inheritdoc */
  override def hasNext: Boolean = {
    prepareNext()
  }

  /** @inheritdoc */
  override def next(): UnfilteredRowIterator = {
    if (nextData.isEmpty) prepareNext()
    val result = nextData.orNull
    nextData = None
    result
  }

  /** @inheritdoc */
  override def remove() = {
    throw new UnsupportedOperationException
  }

  /** @inheritdoc */
  override def close() = {
    try nextData.foreach(_.close()) finally documents.close()
  }

  protected def prepareNext(): Boolean

  protected def read(key: DecoratedKey, filter: ClusteringIndexFilter): UnfilteredRowIterator = {
    // Return a LAZY partition iterator: defer the actual SinglePartitionReadCommand execution
    // (queryMemtableAndDisk) until Cassandra first consumes this partition's rows/metadata.
    //
    // Why this matters: Cassandra wraps the whole IndexReader with a single CheckForAbort
    // StoppingTransformation (via ReadCommand.withStateTracking). That transformation may be
    // attached to only one partition's BaseRows at a time; its onPartitionClose() resets the
    // reference, and the response serializer closes partition N before it calls hasNext()/next()
    // for partition N+1. Core's own range scans rely on each partition being opened lazily, one at
    // a time, so partition N's BaseRows wrapper is always closed before N+1 is attached.
    //
    // The skinny/wide readers call read() from prepareNext() during hasNext(), i.e. they look ahead
    // and would eagerly OPEN partition N+1 while N is still open -- making CheckForAbort attach to a
    // second BaseRows and throw "Attempted to attach 2nd different BaseRows in StoppingTransformation"
    // (surfacing as ReadFailure 1300), notably on COUNT(*) and full scans that never stop early.
    // Deferring the real read until consumption keeps exactly one partition open at a time.
    new LazilyInitializedUnfilteredRowIterator(key) {
      override protected def initializeIterator(): UnfilteredRowIterator =
        SinglePartitionReadCommand.create(
          metadataVal,
          command.nowInSec,
          command.columnFilter,
          command.rowFilter,
          command.limits,
          key,
          filter).queryMemtableAndDisk(table, controller)
    }
  }


}
