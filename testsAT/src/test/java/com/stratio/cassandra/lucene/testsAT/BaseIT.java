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
package com.stratio.cassandra.lucene.testsAT;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.Version;
import com.github.nosan.embedded.cassandra.WorkingDirectoryDestroyer;
import com.github.nosan.embedded.cassandra.commons.function.IOSupplier;
import com.github.nosan.embedded.cassandra.commons.logging.Slf4jLogger;
import com.stratio.cassandra.lucene.testsAT.util.CassandraConnection;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BaseIT {

    public static final Logger logger = LoggerFactory.getLogger("TEST");

    public static Cassandra cassandra;

    @BeforeClass
    public static void connect() throws InterruptedException {
        cassandra = BaseIT.getCassandra();
        BaseIT.cassandra.start();
        CassandraConnection.connect();
    }

    @AfterClass
    public static void destroy() {
        BaseIT.cassandra.stop();
    }

    private <T> void assertPure(String msg, int count, T expected, Callable<T> callable) throws Exception {
        if (count > 0) {
            T actual = callable.call();
            Assert.assertEquals(msg, expected, actual);
            assertPure(msg, count - 1, actual, callable);
        }
    }

    protected <T> void assertPure(String msg, Callable<T> callable) throws Exception {
        assertPure(msg, 10, callable.call(), callable);
    }

    private static Cassandra getCassandra() {
        CassandraBuilder builder = new CassandraBuilder();

        builder.version(Version.parse("4.0-beta4"));
        builder.jvmOptions("-Xmx1g");
        builder.jvmOptions("-Xms1g");
        builder.workingDirectory(new IOSupplier<Path>() {
            @Override
            public Path get() throws IOException {
                return Paths.get("/tmp/cassandra-test/");
            }
        });

        builder.workingDirectoryDestroyer(WorkingDirectoryDestroyer.deleteOnly("data"));

        return builder.build();
    }
}
