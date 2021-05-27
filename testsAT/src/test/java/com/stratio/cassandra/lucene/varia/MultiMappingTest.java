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
package com.stratio.cassandra.lucene.varia;

import static com.stratio.cassandra.lucene.builder.Builder.dateMapper;
import static com.stratio.cassandra.lucene.builder.Builder.field;
import static com.stratio.cassandra.lucene.builder.Builder.match;
import static com.stratio.cassandra.lucene.builder.Builder.stringMapper;

import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */

public class MultiMappingTest extends BaseTest {

    private static CassandraUtils utils;

    @BeforeAll
    public static void before() {
        utils = CassandraUtils.builder("sort_alias")
            .withPartitionKey("key")
            .withColumn("key", "int")
            .withColumn("text", "text", stringMapper())
            .withColumn("map", "map<text, text>", null)
            .withMapper("alias_text", dateMapper().pattern("dd-MM-yyyy").column("text"))
            .build()
            .createKeyspace()
            .createTable()
            .createIndex()
            .insert(new String[]{"key", "text"}, new Object[]{1, "01-01-2014"})
            .insert(new String[]{"key", "text"}, new Object[]{2, "02-01-2013"})
            .insert(new String[]{"key", "text"}, new Object[]{3, "03-01-2012"})
            .insert(new String[]{"key", "text"}, new Object[]{4, "04-01-2011"})
            .refresh();
    }

    @AfterAll
    public static void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void testSimpleQuery() {
        utils.query(match("text", "02-01-2013")).check(1);
    }

    @Test
    public void testAliasQuery() {
        utils.query(match("alias_text", "02-01-2013")).check(1);
    }

    @Test
    public void testSimpleFilter() {
        utils.filter(match("text", "02-01-2013")).check(1);
    }

    @Test
    public void testAliasFilter() {
        utils.filter(match("alias_text", "02-01-2013")).check(1);
    }

    @Test
    public void testSimpleSort() {
        utils.select()
            .sort(field("text"))
            .checkOrderedColumns("text", "01-01-2014", "02-01-2013", "03-01-2012", "04-01-2011");
    }

    @Test
    public void testAliasSort() {
        utils.select()
            .sort(field("alias_text"))
            .checkOrderedColumns("text", "04-01-2011", "03-01-2012", "02-01-2013", "01-01-2014");
    }
}
