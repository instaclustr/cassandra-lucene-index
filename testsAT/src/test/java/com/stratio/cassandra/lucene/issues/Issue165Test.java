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
package com.stratio.cassandra.lucene.issues;

import static com.stratio.cassandra.lucene.builder.Builder.integerMapper;
import static com.stratio.cassandra.lucene.builder.Builder.stringMapper;

import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.jupiter.api.Test;

/**
 * Create an index over a table with ascendant and descendant clustering order(<a href="https://github.com/Stratio/cassandra-lucene-index/issues/165">issue
 * 165</a>)
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */

public class Issue165Test extends BaseTest {

    @Test
    public void testUdtWithDescendingOrderInClusteringKey() {
        CassandraUtils.builder("issue_165").withTable("test")
            .withIndexName("idx")
            .withUDT("location", "street", "text")
            .withUDT("location", "city", "text")
            .withUDT("location", "zip", "int")
            .withColumn("login", "text", stringMapper())
            .withColumn("mailing", "frozen<location>")
            .withIndexColumn("lucene")
            .withColumn("start", "date", null)
            .withColumn("stop", "date", null)
            .withPartitionKey("login")
            .withClusteringKey("mailing")
            .withClusteringOrder("mailing", false)
            .withMapper("mailing.zip", integerMapper())
            .build()
            .createKeyspace()
            .createUDTs()
            .createTable()
            .createIndex()
            .dropKeyspace();
    }

    @Test
    public void testUdtWithAscendingOrderInClusteringKey() {
        CassandraUtils.builder("issue_165").withTable("test")
            .withIndexName("idx")
            .withUDT("location", "street", "text")
            .withUDT("location", "city", "text")
            .withUDT("location", "zip", "int")
            .withColumn("login", "text", stringMapper())
            .withColumn("mailing", "frozen<location>")
            .withIndexColumn("lucene")
            .withColumn("start", "date", null)
            .withColumn("stop", "date", null)
            .withPartitionKey("login")
            .withClusteringKey("mailing")
            .withClusteringOrder("mailing", true)
            .withMapper("mailing.zip", integerMapper())
            .build()
            .createKeyspace()
            .createUDTs()
            .createTable()
            .createIndex()
            .dropKeyspace();
    }

    @Test
    public void testWithDescendingOrderInClusteringKey() {
        CassandraUtils.builder("issue_165").withTable("test")
            .withIndexName("idx")
            .withColumn("login", "text", stringMapper())
            .withColumn("street", "text", stringMapper())
            .withColumn("zip", "int", integerMapper())
            .withIndexColumn("lucene")
            .withColumn("start", "date", null)
            .withColumn("stop", "date", null)
            .withPartitionKey("login")
            .withClusteringKey("street")
            .withClusteringOrder("street", false)
            .build()
            .createKeyspace()
            .createTable()
            .createIndex()
            .dropKeyspace();
    }

    @Test
    public void testWithAscendingOrderInClusteringKey() {
        CassandraUtils.builder("issue_165").withTable("test")
            .withIndexName("idx")
            .withColumn("login", "text", stringMapper())
            .withColumn("street", "text", stringMapper())
            .withColumn("zip", "int", integerMapper())
            .withIndexColumn("lucene")
            .withColumn("start", "date", null)
            .withColumn("stop", "date", null)
            .withPartitionKey("login")
            .withClusteringKey("street")
            .withClusteringOrder("street", true)
            .build()
            .createKeyspace()
            .createTable()
            .createIndex()
            .dropKeyspace();
    }
}
