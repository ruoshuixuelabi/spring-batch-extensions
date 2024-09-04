package org.springframework.batch.extensions.bigquery.emulator.writer;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.batch.extensions.bigquery.common.BigQueryDataLoader;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.emulator.BaseEmulatorTest;
import org.springframework.batch.extensions.bigquery.writer.BigQueryJsonItemWriter;
import org.springframework.batch.extensions.bigquery.writer.builder.BigQueryJsonItemWriterBuilder;

import java.util.List;
import java.util.stream.StreamSupport;

class JsonWriterTest extends BaseEmulatorTest {

    // TODO add job listener
    @ParameterizedTest
    @ValueSource(strings = {"json-writer-non-existing", "json-writer-pre-created"})
    void testWriteJson(String tableName) throws Exception {
        TableId tableId = TableId.of(TestConstants.DATASET, tableName);

        WriteChannelConfiguration config = WriteChannelConfiguration
                .newBuilder(tableId)
                .setSchema(PersonDto.getBigQuerySchema())
                .setFormatOptions(FormatOptions.json())
                .build();

        performTest(config, tableId);
    }

    // TODO move test to gcloud package
    @Test
    void testWriteJsonWithAutodetection() throws Exception {
        TableId tableId = TableId.of(TestConstants.DATASET, "json-writer-auto-detect");

        WriteChannelConfiguration config = WriteChannelConfiguration
                .newBuilder(tableId)
                .setAutodetect(true)
                .setFormatOptions(FormatOptions.json())
                .build();

        performTest(config, tableId);
    }

    private void performTest(WriteChannelConfiguration config, TableId tableId) throws Exception {
        BigQueryJsonItemWriter<Object> writer = new BigQueryJsonItemWriterBuilder<>()
                .bigQuery(bigQuery)
                .writeChannelConfig(config)
                .build();

        writer.afterPropertiesSet();
        writer.write(BigQueryDataLoader.CHUNK);

        List<FieldValueList> result = StreamSupport
                .stream(bigQuery.listTableData(tableId, BigQuery.TableDataListOption.pageSize(5L)).getValues().spliterator(), false)
                .toList();

        FieldValueList bqRow1 = result.get(0);
        PersonDto expected1 = BigQueryDataLoader.CHUNK.getItems().get(0);
        Assertions.assertEquals(expected1.name(), bqRow1.get(0).getStringValue());
        Assertions.assertEquals(expected1.age(), bqRow1.get(1).getNumericValue().intValue());

        FieldValueList bqRow2 = result.get(1);
        PersonDto expected2 = BigQueryDataLoader.CHUNK.getItems().get(1);
        Assertions.assertEquals(expected2.name(), bqRow2.get(0).getStringValue());
        Assertions.assertEquals(expected2.age(), bqRow2.get(1).getNumericValue().intValue());
    }
}
