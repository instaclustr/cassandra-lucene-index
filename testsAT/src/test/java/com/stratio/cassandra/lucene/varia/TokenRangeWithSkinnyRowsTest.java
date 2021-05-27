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

import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TokenRangeWithSkinnyRowsTest extends BaseTest {

    private static CassandraUtils utils;

    @BeforeAll
    public static void before() {
        utils = CassandraUtils.builder("token_range_skinny_rows")
            .withPartitionKey("integer_1", "ascii_1")
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
            .insert(DataHelper.data1,
                    DataHelper.data2,
                    DataHelper.data3,
                    DataHelper.data4,
                    DataHelper.data5,
                    DataHelper.data6,
                    DataHelper.data7,
                    DataHelper.data8,
                    DataHelper.data9,
                    DataHelper.data10,
                    DataHelper.data11,
                    DataHelper.data12,
                    DataHelper.data13,
                    DataHelper.data14,
                    DataHelper.data15,
                    DataHelper.data16,
                    DataHelper.data17,
                    DataHelper.data18,
                    DataHelper.data19,
                    DataHelper.data20)
            .refresh();
    }

    @AfterAll
    public static void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void testTokenSearch1() {
        utils.searchAll().and("AND TOKEN(integer_1, ascii_1) > TOKEN(1, 'ascii')").check(4);
    }

    @Test
    public void testTokenSearch2() {
        utils.searchAll().and("AND TOKEN(integer_1, ascii_1) >= TOKEN(1, 'ascii')").check(5);
    }

    @Test
    public void testTokenSearch3() {
        utils.searchAll().and("AND TOKEN(integer_1, ascii_1) < TOKEN(1, 'ascii')").check(5);
    }

    @Test
    public void testTokenSearch4() {
        utils.searchAll().and("AND TOKEN(integer_1, ascii_1) <= TOKEN(1, 'ascii')").check(6);
    }

    @Test
    public void testTokenSearch5() {
        utils.searchAll()
            .and("AND TOKEN(integer_1, ascii_1) > TOKEN(1, 'ascii')")
            .and("AND TOKEN(integer_1, ascii_1) < TOKEN(3, 'ascii')")
            .check(3);
    }

    @Test
    public void testTokenSearch6() {
        utils.searchAll()
            .and("AND TOKEN(integer_1, ascii_1) >= TOKEN(1, 'ascii')")
            .and("AND TOKEN(integer_1, ascii_1) < TOKEN(3, 'ascii')")
            .check(4);
    }

    @Test
    public void testTokenSearch7() {
        utils.searchAll()
            .and("AND TOKEN(integer_1, ascii_1) > TOKEN(1, 'ascii')")
            .and("AND TOKEN(integer_1, ascii_1) <= TOKEN(3, 'ascii')")
            .check(4);
    }

    @Test
    public void testTokenSearch8() {
        utils.searchAll()
            .and("AND TOKEN(integer_1, ascii_1) >= TOKEN(1, 'ascii')")
            .and("AND TOKEN(integer_1, ascii_1) <= TOKEN(3, 'ascii')")
            .check(5);
    }
}
