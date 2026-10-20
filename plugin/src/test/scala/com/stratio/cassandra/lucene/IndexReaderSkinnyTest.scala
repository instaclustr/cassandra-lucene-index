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
import org.apache.cassandra.db.filter.ClusteringIndexFilter
import org.apache.cassandra.db.rows.UnfilteredRowIterator
import org.apache.cassandra.db.{DecoratedKey, ReadCommand}
import org.apache.lucene.document.Document
import org.apache.lucene.search.ScoreDoc
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatestplus.junit.JUnitRunner

import scala.collection.mutable

/** Regression test for the fix in [[IndexReaderSkinny]]: a Lucene hit whose row no longer exists
  * (an orphaned index document left after a delete whose SSTables were compacted away) yields an
  * EMPTY partition on re-read. Such empty partitions must be closed and skipped, never returned to
  * Cassandra's read pipeline -- returning them makes `ReadCommand.CheckForAbort` attach to a second
  * `BaseRows` and the replica read fails with `ReadFailure(code=1300)`.
  *
  * The buggy version closed the empty partition but still left it in `nextData` (returning it);
  * these tests fail against that version and pass against the fix.
  */
@RunWith(classOf[JUnitRunner])
class IndexReaderSkinnyTest extends BaseScalaTest {

  /** Test double that bypasses the storage engine: `read` just returns pre-canned partitions. */
  private class TestReader(
      service: IndexServiceSkinny,
      command: ReadCommand,
      documents: DocumentIterator,
      results: mutable.Queue[UnfilteredRowIterator])
    extends IndexReaderSkinny(service, command, null, null, documents) {
    override protected def read(key: DecoratedKey, filter: ClusteringIndexFilter): UnfilteredRowIterator =
      results.dequeue()
  }

  /** A partition mock that records whether it was touched in any way that would force a real
    * (lazy) partition to initialize: isEmpty / hasNext / next / staticRow / partitionLevelDeletion. */
  private def untouchablePartition(): UnfilteredRowIterator = mock(classOf[UnfilteredRowIterator])

  private def verifyNotInitialized(p: UnfilteredRowIterator): Unit = {
    verify(p, never()).isEmpty
    verify(p, never()).hasNext
    verify(p, never()).staticRow()
    verify(p, never()).partitionLevelDeletion()
  }

  private def hit: (Document, ScoreDoc) = (new Document, new ScoreDoc(0, 1.0f))

  test("prepareNext does NOT initialize/inspect the partition during look-ahead") {
    // The partition must be produced but never touched (no isEmpty/hasNext/close), so a real lazy
    // partition stays uninitialized until Cassandra consumes it -- keeping one partition open at a time.
    val documents = mock(classOf[DocumentIterator])
    when(documents.hasNext).thenReturn(true, false)
    when(documents.next).thenReturn(hit)

    val p = untouchablePartition()

    val reader = new TestReader(
      mock(classOf[IndexServiceSkinny]),
      mock(classOf[ReadCommand]),
      documents,
      mutable.Queue(p))

    reader.hasNext shouldBe true     // a hit exists
    reader.next shouldBe p           // the exact partition is returned, untouched
    verifyNotInitialized(p)          // look-ahead never forced it open or closed it
  }

  test("empty (orphaned) partitions are returned lazily, not skipped or closed during look-ahead") {
    // With lazy reads we no longer special-case empty partitions: core opens, finds nothing, and
    // closes them itself. The reader must not call isEmpty/close on them while preparing.
    val documents = mock(classOf[DocumentIterator])
    when(documents.hasNext).thenReturn(true, false)
    when(documents.next).thenReturn(hit)

    val p = untouchablePartition()

    val reader = new TestReader(
      mock(classOf[IndexServiceSkinny]),
      mock(classOf[ReadCommand]),
      documents,
      mutable.Queue(p))

    reader.hasNext shouldBe true
    reader.next shouldBe p
    verify(p, never()).isEmpty       // not inspected
    verify(p, never()).close()       // not closed by the reader during preparation
  }

  test("prepareNext returns false when there are no hits") {
    val documents = mock(classOf[DocumentIterator])
    when(documents.hasNext).thenReturn(false)

    val reader = new TestReader(
      mock(classOf[IndexServiceSkinny]),
      mock(classOf[ReadCommand]),
      documents,
      mutable.Queue())

    reader.hasNext shouldBe false
  }

  test("close closes the prepared-but-not-yet-returned partition and the document iterator") {
    val documents = mock(classOf[DocumentIterator])
    when(documents.hasNext).thenReturn(true, false)
    when(documents.next).thenReturn(hit)

    val prepared = untouchablePartition()

    val reader = new TestReader(
      mock(classOf[IndexServiceSkinny]),
      mock(classOf[ReadCommand]),
      documents,
      mutable.Queue(prepared))

    reader.hasNext shouldBe true     // prepares `prepared` into nextData
    reader.close()
    verify(prepared).close()         // the pending partition is released ...
    verify(documents).close()        // ... and so is the document iterator
  }
}
