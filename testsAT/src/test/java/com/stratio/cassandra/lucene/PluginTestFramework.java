package com.stratio.cassandra.lucene;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Version;
import com.github.nosan.embedded.cassandra.WorkingDirectoryDestroyer;
import com.stratio.cassandra.lucene.util.CassandraConnection;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith({PluginTestFramework.NestedSingleton.class})
public abstract class PluginTestFramework {

    private static final Logger logger = LoggerFactory.getLogger(PluginTestFramework.class);

    static class NestedSingleton implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

        public static final String pluginCassandraVersion = System.getProperty("plugin.version", "4.0-rc1-1.0.1");
        public static final String cassandraVersion = System.getProperty("cassandra.version", "4.0-rc1");

        private static volatile boolean disconnected = false;
        private static volatile boolean initialized = false;
        public static Cassandra cassandra;

        @Override
        public void beforeAll(ExtensionContext context) {
            if (!initialized) {
                initialized = true;

                context.getRoot().getStore(Namespace.GLOBAL).put(this.getClass().getCanonicalName(), this);

                cassandra = getCassandra();
                logger.info("Starting Cassandra");
                cassandra.start();
                logger.info("Cassandra started");

                try {
                    logger.info("Connecting to Cassandra");
                    CassandraConnection.connect();
                } catch (final Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        private static Cassandra getCassandra() {
            CassandraBuilder builder = new CassandraBuilder();

            builder.version(Version.parse(cassandraVersion));
            builder.jvmOptions("-Xmx1g");
            builder.jvmOptions("-Xms1g");
            builder.workingDirectory(() -> Paths.get("/tmp/cassandra-test/"));
            builder.workingDirectoryCustomizers((workingDirectory, version) -> {
                final String pluginDir = System.getProperty("outputDirectory", "../plugin/target");
                final Path pluginJar = Paths.get(pluginDir, "cassandra-lucene-index-plugin-" + pluginCassandraVersion + ".jar");
                Files.copy(pluginJar, workingDirectory.resolve("lib").resolve(pluginJar.getFileName()), REPLACE_EXISTING);
                final Path jtsCoreJar = workingDirectory.resolve("lib").resolve("jts-core.jar");
                if (!Files.exists(jtsCoreJar)) {
                    FileUtils.copyURLToFile(new URL("https://search.maven.org/remotecontent?filepath=com/vividsolutions/jts-core/1.14.0/jts-core-1.14.0.jar"),
                                            jtsCoreJar.toFile());
                }
            });

            builder.addConfigProperties(new HashMap<String, String>() {{
                put("enable_sasi_indexes", "true");
                put("enable_user_defined_functions", "true");
            }});

            builder.workingDirectoryDestroyer(WorkingDirectoryDestroyer.deleteOnly("data"));

            return builder.build();
        }

        @Override
        public void close() {
            logger.info("Disconnecting from Cassandra");
            CassandraConnection.disconnect();
            logger.info("Stopping Cassandra");
            cassandra.stop();
            disconnected = true;
        }
    }
}
