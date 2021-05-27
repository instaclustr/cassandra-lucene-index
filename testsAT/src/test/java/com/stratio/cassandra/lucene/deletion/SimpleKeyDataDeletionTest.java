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
package com.stratio.cassandra.lucene.deletion;

import static com.stratio.cassandra.lucene.builder.Builder.wildcard;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SimpleKeyDataDeletionTest extends BaseTest {

    private CassandraUtils utils;

    @BeforeEach
    public void before() {
        utils = CassandraUtils.builder("simple_key_data_deletion")
            .withPartitionKey("integer_1")
            .withClusteringKey()
            .withColumn("ascii_1", "ascii")
            .withColumn("bigint_1", "bigint")
            .withColumn("blob_1", "blob")
            .withColumn("boolean_1", "boolean")
            .withColumn("decimal_1", "decimal")
            .withColumn("date_1", "timestamp")
            .withColumn("double_1", "double")
            .withColumn("float_1", "float")
            .withColumn("integer_1", "int")
            .withColumn("inet_1", "inet")
            .withColumn("text_1", "text")
            .withColumn("varchar_1", "varchar")
            .withColumn("uuid_1", "uuid")
            .withColumn("timeuuid_1", "timeuuid")
            .withColumn("list_1", "list<text>")
            .withColumn("set_1", "set<text>")
            .withColumn("map_1", "map<text,text>")
            .build()
            .createKeyspace()
            .createTable()
            .createIndex()
            .insert(DataHelper.data1, DataHelper.data2, DataHelper.data3, DataHelper.data4, DataHelper.data5);
    }

    @AfterEach
    public void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void testColumnDeletion() {
        List<Row> rows = utils.delete("bigint_1")
            .where("integer_1", 1)
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .get();

        assertEquals(5, rows.size());

        int integerValue;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            if (integerValue == 1) {
                assertTrue("Must be null!", row.isNull("bigint_1"));
            }
        }
    }

    @Test
    public void testMapElementDeletion() {
        List<Row> rows = utils.delete("map_1['k1']")
            .where("integer_1", 1)
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .get();

        assertEquals(5, rows.size());

        int integerValue;
        Map<String, String> mapValue = null;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            if (integerValue == 1) {
                mapValue = row.getMap("map_1", String.class, String.class);
            }
        }

        assertNotNull("Must not be null!", mapValue);
        assertNull("Must be null!", mapValue.get("k1"));
    }

    @Test
    public void testListElementDeletion() {
        List<Row> rows = utils.delete("list_1[0]")
            .where("integer_1", 1)
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .get();

        Assertions.assertEquals(5, rows.size());

        int integerValue;
        List<String> listValue = null;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            if (integerValue == 1) {
                listValue = row.getList("list_1", String.class);
            }
        }

        assertNotNull("Must not be null!", listValue);
        assertEquals("Length unexpected", 1, listValue.size());
    }

    @Test
    public void testTotalPartitionDeletion() {
        utils.delete().where("integer_1", 1).refresh().filter(wildcard("ascii_1", "*")).check(4);
    }
}
