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

import static com.stratio.cassandra.lucene.builder.Builder.all;
import static com.stratio.cassandra.lucene.builder.Builder.field;
import static com.stratio.cassandra.lucene.builder.Builder.integerMapper;

import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */

public class InOperatorWithSkinnyRowsTest extends BaseTest {

    private static final int NUM_PARTITIONS = 10;
    private static CassandraUtils utils;

    @BeforeAll
    public static void before() {
        utils = CassandraUtils.builder("in_operator_with_skinny_rows")
            .withUseNewQuerySyntax(true)
            .withPartitionKey("pk")
            .withColumn("pk", "int", integerMapper())
            .withColumn("rc", "int", integerMapper())
            .build()
            .createKeyspace()
            .createTable()
            .createIndex();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            utils.insert(new String[]{"pk", "rc"}, new Object[]{i, i});
        }
        utils.refresh();
    }

    @AfterAll
    public static void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void testPartitionKeyIn() {
        utils.searchAll().fetchSize(4).and("AND pk IN (0, 5, 9)").checkOrderedColumns("rc", 0, 5, 9);
    }

    @Test
    public void testReversedPartitionKeyIn() {
        utils.searchAll().fetchSize(4).and("AND pk IN (9, 5, 0)").checkOrderedColumns("rc", 0, 5, 9);
    }

    @Test
    public void testQueryWithIn() {
        utils.query(all()).fetchSize(4).and("AND pk IN (9, 5, 0)").checkOrderedColumns("rc", 5, 0, 9);
    }

    @Test
    public void testSortWithIn() {
        utils.sort(field("pk")).fetchSize(4).and("AND pk IN (9, 5, 0)").checkOrderedColumns("rc", 0, 5, 9);
    }
}
