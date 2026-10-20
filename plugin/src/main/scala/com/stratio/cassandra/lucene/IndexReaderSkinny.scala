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

/** [[IndexReader]] for skinny rows.
  *
  * @param service    the index service
  * @param command    the read command
  * @param table      the base table
  * @param controller the read execution controller
  * @param documents  the documents iterator
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexReaderSkinny(
    service: IndexServiceSkinny,
    command: ReadCommand,
    table: ColumnFamilyStore,
    controller: ReadExecutionController,
    documents: DocumentIterator)
  extends IndexReader(command, table, controller, documents) {

  /** @inheritdoc */
  override protected def prepareNext(): Boolean = {
    if (nextData.isEmpty && documents.hasNext) {
      val nextDoc = documents.next
      val key = service.decoratedKey(nextDoc._1)
      val filter = command.clusteringIndexFilter(key)
      // read() returns a lazily-initialized partition (see IndexReader.read): do NOT call isEmpty
      // here. isEmpty would force the partition open during hasNext()/prepareNext() look-ahead, so
      // two partitions would be open at once and Cassandra's single CheckForAbort would attach to a
      // second BaseRows and fail (ReadFailure 1300). Empty (orphaned) partitions are harmless when
      // produced lazily: core opens, finds no rows, closes them (firing onPartitionClose) and moves
      // on, exactly as it does for empty partitions in a normal range scan.
      nextData = Some(read(key, filter))
    }
    nextData.isDefined
  }

}
