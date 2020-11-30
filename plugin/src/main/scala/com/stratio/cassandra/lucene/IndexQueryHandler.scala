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

import java.lang.reflect.{Field, Modifier}
import java.nio.ByteBuffer

import com.stratio.cassandra.lucene.IndexQueryHandler._
import com.stratio.cassandra.lucene.partitioning.Partitioner
import com.stratio.cassandra.lucene.util.{Logging, TimeCounter}
import org.apache.cassandra.cql3._
import org.apache.cassandra.cql3.selection.Selection.Selectors
import org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull
import org.apache.cassandra.cql3.statements.schema.IndexTarget
import org.apache.cassandra.cql3.statements.{BatchStatement, SelectStatement}
import org.apache.cassandra.db.SinglePartitionReadCommand.Group
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.RowFilter.{CustomExpression, Expression}
import org.apache.cassandra.db.partitions.PartitionIterator
import org.apache.cassandra.exceptions.InvalidRequestException
import org.apache.cassandra.service.{ClientState, LuceneStorageProxy, QueryState}
import org.apache.cassandra.transport.messages.ResultMessage
import org.apache.cassandra.transport.messages.ResultMessage.{Prepared, Rows}
import org.apache.cassandra.utils.{FBUtilities, MD5Digest}

import scala.collection.JavaConverters._
import scala.collection.mutable


/** [[QueryHandler]] to be used with Lucene searches.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */

object MyLogger extends Logging {
  def log(): Unit = {
    synchronized {
      val fullClassName = Thread.currentThread.getStackTrace()(2).getClassName
      val className = fullClassName.substring(fullClassName.lastIndexOf(".") + 1)
      val methodName = Thread.currentThread.getStackTrace()(2).getMethodName
      val lineNumber = Thread.currentThread.getStackTrace()(2).getLineNumber
      logger.info(className + "." + methodName + "():" + lineNumber)
    }
  }
}

class IndexQueryHandler extends QueryHandler with Logging {

  type Payload = java.util.Map[String, ByteBuffer]

  /** @inheritdoc */
  override def prepare(query: String, state: ClientState, payload: Payload): ResultMessage.Prepared = {
    QueryProcessor.instance.prepare(query, state, payload)
  }

  /** @inheritdoc */
  override def getPrepared(id: MD5Digest): QueryHandler.Prepared = {
    QueryProcessor.instance.getPrepared(id)
  }

  override def parse(queryString: String, queryState: QueryState, options: QueryOptions): CQLStatement = {
    QueryProcessor.parseStatement(queryString, queryState.getClientState)
  }

  /** @inheritdoc */
  override def processBatch(
      statement: BatchStatement,
      state: QueryState,
      options: BatchQueryOptions,
      payload: Payload,
      queryStartNanoTime: Long): ResultMessage = {
    QueryProcessor.instance.processBatch(statement, state, options, payload, queryStartNanoTime)
  }

  /** @inheritdoc */
  override def processPrepared(
      statement: CQLStatement,
      state: QueryState,
      options: QueryOptions,
      payload: Payload,
      queryStartNanoTime: Long): ResultMessage = {
    QueryProcessor.metrics.preparedStatementsExecuted.inc()
    processStatement(statement, state, options, queryStartNanoTime)
  }

  override def process(statement: CQLStatement,
                       state: QueryState,
                       options: QueryOptions,
                       customPayload: java.util.Map[String, ByteBuffer],
                       queryStartNanoTime: Long): ResultMessage = {
    processStatement(statement, state, options, queryStartNanoTime)
  }

  def processStatement(
      statement: CQLStatement,
      state: QueryState,
      options: QueryOptions,
      queryStartNanoTime: Long): ResultMessage = {

    //logger.info(s"Process $statement @CL.${options.getConsistency}")
    MyLogger.log()
//    val clientState = state.getClientState
//    statement.authorize(clientState)
//    statement.validate(clientState)

    MyLogger.log()

    // Intercept Lucene index searches
    statement match {
      case select: SelectStatement =>
        MyLogger.log()
        val expressions = luceneExpressions(select, options)
        if (expressions.nonEmpty) {
          MyLogger.log()
          val time = TimeCounter.start
          try {
            MyLogger.log()
            return executeLuceneQuery(select, state, options, expressions, queryStartNanoTime)
          } catch {
            case e: ReflectiveOperationException => throw new IndexException(e)
          } finally {
            MyLogger.log()
            logger.info(s"Lucene search total time: $time\n")
          }
        }
      case _ => MyLogger.log()
    }
    MyLogger.log()
    execute(statement, state, options, queryStartNanoTime)
  }

  def luceneExpressions(
      select: SelectStatement,
      options: QueryOptions): Map[Expression, Index] = {
    val map = mutable.LinkedHashMap.empty[Expression, Index]
    val expressions = select.getRowFilter(options).getExpressions

    MyLogger.log()

    if (select.keyspace() == "system_virtual_schema") {
      return Map.empty
    }

    val cfs = Keyspace.open(select.keyspace).getColumnFamilyStore(select.columnFamily)
    MyLogger.log()
    val indexes = cfs.indexManager.listIndexes.asScala.collect { case index: Index => index }
    MyLogger.log()
    expressions.forEach {
      case expression: CustomExpression =>
        MyLogger.log()
        val clazz = expression.getTargetIndex.options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME)
        if (clazz == classOf[Index].getCanonicalName) {
          val index = cfs.indexManager.getIndex(expression.getTargetIndex).asInstanceOf[Index]
          MyLogger.log()
          map += expression -> index
          MyLogger.log()
        }
      case expr: Expression =>
        MyLogger.log()
        indexes.filter(_.supportsExpression(expr.column, expr.operator)).foreach(map.put(expr, _))
    }
    map.toMap
  }

  def execute(
      statement: CQLStatement,
      state: QueryState,
      options: QueryOptions,
      queryStartNanoTime: Long): ResultMessage = {
    MyLogger.log()
    val result = statement.execute(state, options, queryStartNanoTime)
    if (result == null) new ResultMessage.Void else result
  }

  def executeLuceneQuery(
      select: SelectStatement,
      state: QueryState,
      options: QueryOptions,
      expressions: Map[Expression, Index],
      queryStartNanoTime: Long): ResultMessage = {

    if (expressions.size > 1) {
      throw new InvalidRequestException(
        "Lucene index only supports one search expression per query.")
    }

    if (select.getPerPartitionLimit(options) < Integer.MAX_VALUE) {
      throw new InvalidRequestException("Lucene index doesn't support PER PARTITION LIMIT")
    }

    // Validate expression
    val (expression, index) = expressions.head
    val search = index.validate(expression)

    // Get partitioner
    val partitioner = index.service.partitioner

    // Get paging info
    val limit = select.getLimit(options)
    val page = if (select.getSelection.isAggregate && options.getPageSize <= 0)
      SelectStatement.DEFAULT_PAGE_SIZE else options.getPageSize

    // Take control of paging if there is paging and the query requires post processing
    if (search.requiresPostProcessing && page > 0 && page < limit) {
      executeSortedLuceneQuery(select, state, options, partitioner, queryStartNanoTime)
    } else {
      execute(select, state, options, queryStartNanoTime)
    }
  }

  def executeSortedLuceneQuery(
      select: SelectStatement,
      state: QueryState,
      options: QueryOptions,
      partitioner: Partitioner,
      queryStartNanoTime: Long): Rows = {

    // Check consistency level
    val consistency = options.getConsistency
    checkNotNull(consistency, "Invalid empty consistency level")
    consistency.validateForRead(select.keyspace)

    val filter = select.queriedColumns()
    val now = FBUtilities.nowInSeconds
    val limit = select.getLimit(options)
    val userPerPartitionLimit = select.getPerPartitionLimit(options)
    val page = options.getPageSize

    // Read paging state and write it to query
    val pagingState = IndexPagingState.build(options.getPagingState, limit)
    val remaining = Math.min(page, pagingState.remaining)
    val query = select.getQuery(options, filter, now, remaining, userPerPartitionLimit, page)
    pagingState.rewrite(query)

    // Read data
    val data = query match {
      case group: Group if group.queries.size > 1 =>
        LuceneStorageProxy.read(group, consistency, queryStartNanoTime)
      case _ => query.execute(consistency, state.getClientState, queryStartNanoTime)
    }

    val selectors = select.getSelection.newSelectors(options)

    // Process data updating paging state
    try {
      val processedData = pagingState.update(query, data, consistency, partitioner)
      val rows = processResults.invoke(
        select,
        processedData,
        options,
        selectors,
        now.asInstanceOf[AnyRef],
        page.asInstanceOf[AnyRef]).asInstanceOf[Rows]
      rows.result.metadata.setHasMorePages(pagingState.toPagingState)
      rows
    } finally {
      if (data != null) data.close()
    }
  }
}

/** Companion object for [[IndexQueryHandler]]. */
object IndexQueryHandler {

  val processResults = classOf[SelectStatement].getDeclaredMethod(
    "processResults", classOf[PartitionIterator], classOf[QueryOptions], classOf[Selectors], classOf[Int], classOf[Int])
  processResults.setAccessible(true)

  /** Sets this query handler as the Cassandra CQL query handler, replacing the previous one. */
  def activate(): Unit = {
    this.synchronized {
      if (!ClientState.getCQLQueryHandler.isInstanceOf[IndexQueryHandler]) {
        try {
          val field = classOf[ClientState].getDeclaredField("cqlQueryHandler")
          field.setAccessible(true)
          val modifiersField = classOf[Field].getDeclaredField("modifiers")
          modifiersField.setAccessible(true)
          modifiersField.setInt(field, field.getModifiers & ~Modifier.FINAL)
          field.set(null, new IndexQueryHandler)
        } catch {
          case e: Exception => throw new IndexException("Unable to set Lucene CQL query handler", e)
        }
      }
    }
  }

}
