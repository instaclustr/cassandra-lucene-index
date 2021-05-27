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
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Stream;

import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.builder.index.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.CassandraUtilsBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class MultipleColumnRejectTypesIndexCreationTest extends BaseTest {

    private CassandraUtils utils;
    private CassandraUtilsBuilder builder;

    @ParameterizedTest
    @ArgumentsSource(TestArguments.class)
    public void test(final TestData data) {
        builder = CassandraUtils.builder(DataHelper.buildTableName(data.mapperName, data.cqlType))
            .withIndexColumn(null)
            .withUseNewQuerySyntax(true)
            .withPartitionKey("pk")
            .withColumn("pk", "int", null);

        for (String columnName : data.requiredColumnNames) {
            builder = builder.withColumn(columnName, data.cqlType, null);
        }

        utils = builder.withTable(DataHelper.buildTableName(data.mapperName, data.cqlType))
            .withIndexName(DataHelper.buildTableName(data.mapperName, data.cqlType))
            .withMapper(data.mapperName, data.mapper)
            .build()
            .createKeyspace()
            .createTable()
            .createIndex(InvalidConfigurationInQueryException.class, data.expectedExceptionMessage);
    }

    @AfterEach
    public void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    public static class TestArguments implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            final List<Arguments> possibleValues = new ArrayList<>();

            for (final Mapper mapper : DataHelper.multipleColumnMappersAcceptedTypes.keySet().stream().collect(toList())) {
                final String mapperName = mapper.getClass().getSimpleName();
                for (String rejectType : Sets.difference(DataHelper.ALL_CQL_TYPES, DataHelper.multipleColumnMappersAcceptedTypes.get(mapper)).immutableCopy()) {
                    possibleValues.add(Arguments.of(new TestData(mapperName,
                                                                 mapper,
                                                                 DataHelper.multipleColumnMapperRequiredColumnNames.get(mapper.toString()),
                                                                 rejectType,
                                                                 DataHelper.buildIndexMessage(mapperName, rejectType, DataHelper.multipleColumnMapperInvalidColumnName.get(mapperName)))));
                }
            }

            for (Mapper mapper : DataHelper.multipleColumnMappersAcceptedTypes.keySet()) {
                String mapperName = mapper.getClass().getSimpleName();
                for (String acceptedType : DataHelper.multipleColumnMappersAcceptedTypes.get(mapper)) {

                    possibleValues.add(Arguments.of(new TestData(mapperName,
                                                                 mapper,
                                                                 DataHelper.multipleColumnMapperRequiredColumnNames.get(mapper.toString()),
                                                                 DataHelper.listComposedType(acceptedType),
                                                                 DataHelper.buildIndexMessage(mapperName, DataHelper.listComposedType(acceptedType), DataHelper.multipleColumnMapperInvalidColumnName.get(mapperName)))));

                    possibleValues.add(Arguments.of(new TestData(mapperName,
                                                                 mapper,
                                                                 DataHelper.multipleColumnMapperRequiredColumnNames.get(mapper.toString()),
                                                                 DataHelper.setComposedType(acceptedType),
                                                                 DataHelper.buildIndexMessage(mapperName, DataHelper.setComposedType(acceptedType), DataHelper.multipleColumnMapperInvalidColumnName.get(mapperName)))));

                    possibleValues.add(Arguments.of(new TestData(mapperName,
                                                                 mapper,
                                                                 DataHelper.multipleColumnMapperRequiredColumnNames.get(mapper.toString()),
                                                                 DataHelper.mapComposedType(acceptedType),
                                                                 DataHelper.buildIndexMessage(mapperName, DataHelper.mapComposedType(acceptedType), DataHelper.multipleColumnMapperInvalidColumnName.get(mapperName)))));
                }
            }

            return possibleValues.stream();
        }
    }

    public static class TestData {

        public final String mapperName;
        public final Mapper mapper;
        public final String cqlType;
        public final Set<String> requiredColumnNames;
        public final String expectedExceptionMessage;

        public TestData(final String mapperName,
                        final Mapper mapper,
                        final Set<String> requiredColumnNames,
                        final String cqlType,
                        final String expectedExceptionMessage) {
            this.mapperName = mapperName;
            this.mapper = mapper;
            this.cqlType = cqlType;
            this.requiredColumnNames = requiredColumnNames;
            this.expectedExceptionMessage = expectedExceptionMessage;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", TestData.class.getSimpleName() + "[", "]")
                .add("mapperName='" + mapperName + "'")
                .add("cqlType='" + cqlType + "'")
                .add("requiredColumnNames=" + requiredColumnNames)
                .add("expectedExceptionMessage='" + expectedExceptionMessage + "'")
                .toString();
        }
    }
}
