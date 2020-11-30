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
package com.stratio.cassandra.lucene.mapping

import java.nio.ByteBuffer

import com.stratio.cassandra.lucene.mapping.KeyMapper.FIELD_NAME
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter
import org.apache.cassandra.db.marshal.CompositeType
import org.apache.cassandra.db.{Clustering, DecoratedKey}
import org.apache.cassandra.schema.TableMetadata
import org.apache.lucene.document.{Field, StringField}
import org.apache.lucene.index.{IndexableField, Term}
import org.apache.lucene.search.BooleanClause.Occur.SHOULD
import org.apache.lucene.search.{BooleanQuery, Query, TermQuery}
import org.apache.lucene.util.BytesRef

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/** Class for several primary key mappings between Cassandra and Lucene.
  *
  * @param metadata the indexed table metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class KeyMapper(metadata: TableMetadata) {

  /** The clustering key comparator */
  val clusteringComparator = metadata.comparator

  /** A composite type composed by the types of the clustering key */
  val clusteringType = CompositeType.getInstance(clusteringComparator.subtypes)

  /** The type of the primary key, which is composed by token and clustering key types. */
  val keyType = CompositeType.getInstance(metadata.partitionKeyType, clusteringType)

  /** Returns a [[ByteBuffer]] representing the specified clustering key
    *
    * @param clustering the clustering key
    * @return the byte buffer representing `clustering`
    */
  private def byteBuffer(clustering: Clustering[_]): ByteBuffer = {
    clusteringType.decompose(clustering.getRawValues)
  }

  /** Returns the Lucene [[IndexableField]] representing the specified primary key.
    *
    * @param key        the partition key
    * @param clustering the clustering key
    * @return a indexable field
    */
  def indexableField(key: DecoratedKey, clustering: Clustering[_]): IndexableField = {
    new StringField(FIELD_NAME, bytesRef(key, clustering), Field.Store.NO)
  }

  /** Returns the Lucene term representing the specified primary.
    *
    * @param key        a partition key
    * @param clustering a clustering key
    * @return the Lucene term representing the primary key
    */
  def term(key: DecoratedKey, clustering: Clustering[_]): Term = {
    new Term(FIELD_NAME, bytesRef(key, clustering))
  }

  private def bytesRef(key: DecoratedKey, clustering: Clustering[_]): BytesRef = {
    val a = new ListBuffer[ByteBuffer]()
    a += key.getKey
    a ++= clustering.getBufferArray
    ByteBufferUtils.bytesRef(ByteBufferUtils.compose(a.toArray : _*))
  }

  /** Returns a Lucene [[Query]] to retrieve the row with the specified primary key.
    *
    * @param key        a partition key
    * @param clustering a clustering key
    * @return the Lucene query
    */
  def query(key: DecoratedKey, clustering: Clustering[_]): Query = {
    new TermQuery(term(key, clustering))
  }

  /** Returns a Lucene [[Query]] to retrieve all the rows in the specified clustering names filter.
    *
    * @param key    a partition key
    * @param filter a names filter
    * @return the Lucene query
    */
  def query(key: DecoratedKey, filter: ClusteringIndexNamesFilter): Query = {
    (new BooleanQuery.Builder /: filter.requestedRows.asScala) ((builder, clustering) => builder.add(query(key, clustering), SHOULD)).build()
  }

}

/** Companion object for [[KeyMapper]]. */
object KeyMapper {

  /** The Lucene field name. */
  val FIELD_NAME = "_key"

}
