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

  private def partition(empty: Boolean): UnfilteredRowIterator = {
    val it = mock(classOf[UnfilteredRowIterator])
    when(it.isEmpty).thenReturn(empty)
    it
  }

  private def hit: (Document, ScoreDoc) = (new Document, new ScoreDoc(0, 1.0f))

  test("prepareNext skips and closes empty (orphaned) partitions and returns the live one") {
    val documents = mock(classOf[DocumentIterator])
    when(documents.hasNext).thenReturn(true, true, true, false)
    when(documents.next).thenReturn(hit, hit, hit)

    val empty1 = partition(empty = true)
    val empty2 = partition(empty = true)
    val live = partition(empty = false)

    val reader = new TestReader(
      mock(classOf[IndexServiceSkinny]),
      mock(classOf[ReadCommand]),
      documents,
      mutable.Queue(empty1, empty2, live))

    reader.hasNext shouldBe true         // there IS a live result to return
    reader.next shouldBe live            // and it is the live partition, not an empty one
    verify(empty1).close()               // both orphaned partitions were closed ...
    verify(empty2).close()
    verify(live, never()).close()        // ... and the live one was not closed while preparing
  }

  test("prepareNext returns false when every hit is an empty (orphaned) partition") {
    val documents = mock(classOf[DocumentIterator])
    when(documents.hasNext).thenReturn(true, false)
    when(documents.next).thenReturn(hit)

    val empty = partition(empty = true)

    val reader = new TestReader(
      mock(classOf[IndexServiceSkinny]),
      mock(classOf[ReadCommand]),
      documents,
      mutable.Queue(empty))

    reader.hasNext shouldBe false        // nothing to return
    verify(empty).close()                // the orphaned partition was closed
  }
}