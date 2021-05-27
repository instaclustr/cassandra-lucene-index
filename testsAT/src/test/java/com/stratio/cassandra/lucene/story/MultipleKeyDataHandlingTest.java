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
package com.stratio.cassandra.lucene.story;

import static com.stratio.cassandra.lucene.builder.Builder.wildcard;

import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MultipleKeyDataHandlingTest extends BaseTest {

    private static CassandraUtils utils;

    @BeforeEach
    public void before() {
        utils = CassandraUtils.builder("multiple_key_data_handling")
            .withPartitionKey("integer_1")
            .withClusteringKey("ascii_1")
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
            .insert(DataHelper.data1, DataHelper.data2, DataHelper.data3, DataHelper.data6, DataHelper.data7, DataHelper.data8, DataHelper.data9, DataHelper.data10)
            .refresh();
    }

    @AfterEach
    public void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void testSingleInsertion() {
        utils.insert(DataHelper.data4)
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(9)
            .insert(DataHelper.data5)
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(10)
            .delete()
            .where("integer_1", 4)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(9)
            .delete()
            .where("integer_1", 5)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(8)
            .delete()
            .where("integer_1", 2)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(7)
            .delete()
            .where("integer_1", 3)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(6)
            .delete()
            .where("integer_1", 1)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(5);
    }

    @Test
    public void testMultipleInsertion() {
        utils.insert(DataHelper.data4, DataHelper.data5)
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(10)
            .delete()
            .where("integer_1", 4)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(9)
            .delete()
            .where("integer_1", 5)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(8)
            .delete()
            .where("integer_1", 2)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(7)
            .delete()
            .where("integer_1", 3)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(6)
            .delete()
            .where("integer_1", 1)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(5);
    }

    @Test
    public void testMultipleDeletion() {
        utils.delete()
            .where("integer_1", 2)
            .and("ascii_1", "ascii")
            .delete()
            .where("integer_1", 3)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(6)
            .delete()
            .where("integer_1", 1)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("ascii_1", "*"))
            .check(5);
    }

    @Test
    public void testUpdate() {
        utils.filter(wildcard("text_1", "text"))
            .check(8)
            .update()
            .set("text_1", "other")
            .where("integer_1", 1)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("text_1", "text"))
            .check(7)
            .filter(wildcard("text_1", "other"))
            .check(1);
    }

    @Test
    public void testInsertWithUpdate() {
        utils.filter(wildcard("text_1", "text"))
            .check(8)
            .update()
            .set("text_1", "new")
            .where("integer_1", 1000)
            .and("ascii_1", "ascii")
            .refresh()
            .filter(wildcard("text_1", "new"))
            .check(1);
    }
}
