package com.stratio.cassandra.lucene.issues;

import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import com.stratio.cassandra.lucene.util.CassandraUtilsSelect;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.stratio.cassandra.lucene.builder.Builder.*;

public class OrderByClauseTest extends BaseTest {

    @Test
    public void test() {
        HashMap<String, String> row1 = new LinkedHashMap<>();
        row1.put("id", "1");
        row1.put("sent_at", "'2025-01-07 15:10:50.506000+0000'");
        row1.put("tags", "'test'");

        HashMap<String, String> row2 = new LinkedHashMap<>();
        row2.put("id", "1");
        row2.put("sent_at", "'2025-01-07 15:10:50.507000+0000'");
        row2.put("tags", "'test'");

        HashMap<String, String> row3 = new LinkedHashMap<>();
        row3.put("id", "1");
        row3.put("sent_at", "'2025-01-07 15:10:51.665000+0000'");
        row3.put("tags", "'test'");

        HashMap<String, String> row4 = new LinkedHashMap<>();
        row4.put("id", "1");
        row4.put("sent_at", "'2025-01-07 15:10:51.666000+0000'");
        row4.put("tags", "'test'");

        HashMap<String, String> row5 = new LinkedHashMap<>();
        row5.put("id", "1");
        row5.put("sent_at", "'2025-01-07 15:11:34.390000+0000'");
        row5.put("tags", "'test'");

        HashMap<String, String> row6 = new LinkedHashMap<>();
        row6.put("id", "1");
        row6.put("sent_at", "'2025-01-08 15:11:34.390000+0000'");
        row6.put("tags", "'test'");

        List<HashMap<String, String>> expectedResultOrder = new ArrayList<>();
        expectedResultOrder.add(row6);
        expectedResultOrder.add(row5);
        expectedResultOrder.add(row4);
        expectedResultOrder.add(row3);
        expectedResultOrder.add(row2);
        expectedResultOrder.add(row1);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSX");

        CassandraUtils.builder("order_by_test").withTable("order_by_test_table")
                .withIndexName("order_by_test_table_index")
                .withColumn("id", "int", integerMapper())
                .withColumn("sent_at", "timestamp", longMapper())
                .withColumn("tags", "text", stringMapper())
                .withPartitionKey("id")
                .withClusteringKey("sent_at")
                .build()
                .createKeyspace()
                .createTable()
                .createIndex()
                .insert(row1, row2, row3, row4, row5, row6)
                .refresh()
                .select()
                .andEq("id", 1)
                .filter(wildcard("tags", "*"))
                .andGte("sent_at", 1736262639000L)
                .orderBy("sent_at", CassandraUtilsSelect.Order.DESC)
                .checkOrderedColumns("sent_at", expectedResultOrder
                        .stream()
                        .map((row) -> {
                                String sentAt = row.get("sent_at");
                                sentAt = sentAt.replace("'", "");
                                return Date.from(Instant.from(formatter.parse(sentAt)));
                        })
                        .toArray()
                )
                .dropKeyspace();
    }
}
