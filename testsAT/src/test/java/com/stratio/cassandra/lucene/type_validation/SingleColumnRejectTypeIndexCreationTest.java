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
package com.stratio.cassandra.lucene.type_validation;

import static com.stratio.cassandra.lucene.type_validation.DataHelper.buildIndexMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Stream;

import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class SingleColumnRejectTypeIndexCreationTest extends BaseTest {

    private static CassandraUtils utils;

    @ParameterizedTest
    @ArgumentsSource(TestArguments.class)
    public void test(TestData data) {
        utils = CassandraUtils.builder(DataHelper.buildTableName(data.mapperName, data.cqlType))
            .withIndexColumn(null)
            .withUseNewQuerySyntax(true)
            .withPartitionKey("pk")
            .withColumn("pk", "int", null)
            .withTable(DataHelper.buildTableName(data.mapperName, data.cqlType))
            .withIndexName(DataHelper.buildTableName(data.mapperName, data.cqlType))
            .withColumn("column", data.cqlType, data.mapper)
            .build()
            .createKeyspace()
            .createTable()
            .createIndex(InvalidConfigurationInQueryException.class, data.expectedExceptionMessage);
    }

    @AfterEach
    public void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    static class TestArguments implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
            final List<Arguments> possibleValues = new ArrayList<>();
            for (final String mapperType : DataHelper.singleColumnMappersAcceptedTypes.keySet()) {
                for (final String rejectType : Sets.difference(DataHelper.ALL_CQL_TYPES, DataHelper.singleColumnMappersAcceptedTypes.get(mapperType)).immutableCopy()) {
                    possibleValues.add(Arguments.of(new TestData(mapperType,
                                                                 DataHelper.mapperByName.get(mapperType),
                                                                 rejectType,
                                                                 DataHelper.buildIndexMessage("column", rejectType))));
                }
            }
            return possibleValues.stream();
        }
    }

    static class TestData {

        public final String mapperName;
        public final Mapper mapper;
        public final String cqlType;
        public final String expectedExceptionMessage;

        public TestData(final String mapperName,
                        final Mapper mapper,
                        final String cqlType,
                        final String expectedExceptionMessage) {
            this.mapperName = mapperName;
            this.mapper = mapper;
            this.cqlType = cqlType;
            this.expectedExceptionMessage = expectedExceptionMessage;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", TestData.class.getSimpleName() + "[", "]")
                .add("mapperName='" + mapperName + "'")
                .add("mapper=" + mapper)
                .add("cqlType='" + cqlType + "'")
                .toString();
        }
    }
}
