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

import static com.stratio.cassandra.lucene.builder.Builder.doubleMapper;
import static com.stratio.cassandra.lucene.builder.Builder.integerMapper;
import static com.stratio.cassandra.lucene.builder.Builder.range;

import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test complex clustering key (<a href="https://github.com/Stratio/cassandra-lucene-index/issues/143">issue 143</a>).
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */

public class Issue143Test extends BaseTest {

    protected static CassandraUtils utils;

    @BeforeAll
    public static void beforeClass() {
        utils = CassandraUtils.builder("issue_143").withTable("waypoints_subset")
            .withIndexName("idx1")
            .withColumn("id", "int", null)
            .withColumn("year", "int", null)
            .withColumn("month", "int", null)
            .withColumn("day", "int", null)
            .withColumn("hour", "int", null)
            .withColumn("minute", "int", null)
            .withColumn("second", "int", null)
            .withColumn("datetime", "timestamp", null)
            .withColumn("driverid", "int", integerMapper())
            .withColumn("ignition", "boolean", null)
            .withColumn("lat", "double", null)
            .withColumn("lon", "double", null)
            .withColumn("odometer", "double", doubleMapper())
            .withColumn("speed", "double", doubleMapper())
            .withPartitionKey("id")
            .withClusteringKey("year", "month", "day", "hour", "minute", "second")
            .build()
            .createKeyspace()
            .createTable()
            .insert(new String[]{"id",
                        "year",
                        "month",
                        "day",
                        "hour",
                        "minute",
                        "second",
                        "speed"},
                    new Object[]{1, 2, 3, 4, 5, 6, 7, 0})
            .insert(new String[]{"id",
                        "year",
                        "month",
                        "day",
                        "hour",
                        "minute",
                        "second",
                        "speed"},
                    new Object[]{1, 2, 3, 4, 5, 6, 8, 10});
    }

    @AfterAll
    public static void afterClass() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void test() {
        utils.createIndex().refresh().filter(range("speed").upper(10)).check(1);
    }
}
