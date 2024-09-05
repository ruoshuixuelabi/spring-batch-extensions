package org.springframework.batch.extensions.bigquery.emulator.writer;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;

class BigQueryEmulatorCsvItemWriterTest extends BaseEmulatorWriterTest {

    @ParameterizedTest
    @ValueSource(strings = {"csv-writer-non-existing", "csv-writer-pre-created"})
    void testWriteCsv(String tableName) throws Exception {
        TableId tableId = TableId.of(TestConstants.DATASET, tableName);

        WriteChannelConfiguration config = WriteChannelConfiguration
                .newBuilder(tableId)
                .setSchema(PersonDto.getBigQuerySchema())
                .setFormatOptions(FormatOptions.csv())
                .build();

        performTest(config, tableId);
    }
}
