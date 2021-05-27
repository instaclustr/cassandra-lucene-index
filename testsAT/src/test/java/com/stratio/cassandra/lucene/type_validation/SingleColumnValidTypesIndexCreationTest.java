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

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Stream;

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
public class SingleColumnValidTypesIndexCreationTest extends BaseTest {

    private static CassandraUtils utils;

    @ParameterizedTest
    @ArgumentsSource(BigDecimalTestArguments.class)
    public void bigDecimalTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(BigIntegerTestArguments.class)
    public void bigIntegerTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(UUIDTestArguments.class)
    public void uuidTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(TextTestArguments.class)
    public void textTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(StringTestArguments.class)
    public void stringTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(LongTestArguments.class)
    public void longTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(IntegerTestArguments.class)
    public void integerTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(InetTestArguments.class)
    public void inetTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(GeoShapeTestArguments.class)
    public void geoShapeTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(FloatTestArguments.class)
    public void floatTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(DoubleTestArguments.class)
    public void doubleTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(DateTestArguments.class)
    public void dateTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(BooleanTestArguments.class)
    public void booleanTest(TestData data) {
        test0(data);
    }

    @ParameterizedTest
    @ArgumentsSource(BlobTestArguments.class)
    public void blobTest(TestData data) {
        test0(data);
    }

    @AfterEach
    public void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    void test0(final TestData data) {
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
            .createIndex();
    }

    private static class BigDecimalTestArguments extends AbstractTestArguments {

        public BigDecimalTestArguments() {
            super("big_decimal");
        }
    }

    private static class BigIntegerTestArguments extends AbstractTestArguments {

        public BigIntegerTestArguments() {
            super("big_integer");
        }
    }

    private static class UUIDTestArguments extends AbstractTestArguments {

        public UUIDTestArguments() {
            super("uuid");
        }
    }

    private static class TextTestArguments extends AbstractTestArguments {

        public TextTestArguments() {
            super("text");
        }
    }

    private static class StringTestArguments extends AbstractTestArguments {

        public StringTestArguments() {
            super("string");
        }
    }

    private static class LongTestArguments extends AbstractTestArguments {

        public LongTestArguments() {
            super("long");
        }
    }

    private static class IntegerTestArguments extends AbstractTestArguments {

        public IntegerTestArguments() {
            super("integer");
        }
    }

    private static class InetTestArguments extends AbstractTestArguments {

        public InetTestArguments() {
            super("inet");
        }
    }

    private static class GeoShapeTestArguments extends AbstractTestArguments {

        public GeoShapeTestArguments() {
            super("geo_shape");
        }
    }

    private static class FloatTestArguments extends AbstractTestArguments {

        public FloatTestArguments() {
            super("float");
        }
    }

    private static class DoubleTestArguments extends AbstractTestArguments {

        public DoubleTestArguments() {
            super("float");
        }
    }

    private static class DateTestArguments extends AbstractTestArguments {

        public DateTestArguments() {
            super("date");
        }
    }

    private static class BooleanTestArguments extends AbstractTestArguments {

        public BooleanTestArguments() {
            super("boolean");
        }
    }

    private static class BlobTestArguments extends AbstractTestArguments {

        public BlobTestArguments() {
            super("blob");
        }
    }

    public static abstract class AbstractTestArguments implements ArgumentsProvider {

        private final String mapperType;

        public AbstractTestArguments(final String mapperType) {
            this.mapperType = mapperType;
        }

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            final List<Arguments> possibleValues = new ArrayList<>();
            for (final String acceptedType : DataHelper.singleColumnMappersAcceptedTypes.get(mapperType)) {
                possibleValues.add(Arguments.of(new TestData(mapperType, DataHelper.mapperByName.get(mapperType), acceptedType)));
                possibleValues.add(Arguments.of(new TestData(mapperType, DataHelper.mapperByName.get(mapperType), DataHelper.listComposedType(acceptedType))));
                possibleValues.add(Arguments.of(new TestData(mapperType, DataHelper.mapperByName.get(mapperType), DataHelper.setComposedType(acceptedType))));
                possibleValues.add(Arguments.of(new TestData(mapperType, DataHelper.mapperByName.get(mapperType), DataHelper.mapComposedType(acceptedType))));
            }

            return possibleValues.stream();
        }
    }

    public static class TestData {

        public final String mapperName;
        public final Mapper mapper;
        public final String cqlType;

        public TestData(final String mapperName,
                        final Mapper mapper,
                        final String cqlType) {
            this.mapperName = mapperName;
            this.mapper = mapper;
            this.cqlType = cqlType;
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
