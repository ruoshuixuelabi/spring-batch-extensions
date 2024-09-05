package org.springframework.batch.extensions.bigquery.unit.writer;

import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.unit.base.AbstractBigQueryTest;
import org.springframework.batch.extensions.bigquery.writer.BigQueryCsvItemWriter;
import org.springframework.batch.extensions.bigquery.writer.BigQueryItemWriterException;
import org.springframework.batch.item.Chunk;

import java.util.stream.Stream;

class BigQueryCsvItemWriterTest extends AbstractBigQueryTest {

    @Test
    void testAfterPropertiesSet_WithoutBigQuery() {
        IllegalArgumentException ex = Assertions.assertThrowsExactly(
                IllegalArgumentException.class, () -> new BigQueryCsvItemWriter<>().afterPropertiesSet()
        );
        Assertions.assertEquals(TestConstants.NO_BIG_QUERY_PROVIDED, ex.getMessage());
    }

    @Test
    void testAfterPropertiesSet_WithoutWriteChannelConfig() {
        BigQueryCsvItemWriter<Object> writer = new BigQueryCsvItemWriter<>();
        writer.setBigQuery(Mockito.mock(BigQuery.class));

        IllegalArgumentException ex = Assertions.assertThrowsExactly(
                IllegalArgumentException.class, writer::afterPropertiesSet
        );
        Assertions.assertEquals(TestConstants.NO_WRITE_CHANNEL_CONFIGURATION_PROVIDED, ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("unsupportedFormats")
    void testAfterPropertiesSet_UnsupportedFormat(FormatOptions formatOptions, String message) {
        WriteChannelConfiguration configuration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, "test"))
                .setFormatOptions(formatOptions)
                .build();

        BigQueryCsvItemWriter<Object> writer = new BigQueryCsvItemWriter<>();
        writer.setBigQuery(Mockito.mock(BigQuery.class));
        writer.setWriteChannelConfig(configuration);

        IllegalArgumentException ex = Assertions.assertThrowsExactly(
                IllegalArgumentException.class, writer::afterPropertiesSet
        );
        Assertions.assertEquals(message, ex.getMessage());
    }

    @Test
    void testAfterPropertiesSet_WithoutFormat() {
        WriteChannelConfiguration configuration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, "test"))
                .setSchema(PersonDto.getBigQuerySchema())
                .build();

        BigQueryCsvItemWriter<Object> writer = new BigQueryCsvItemWriter<>();
        writer.setBigQuery(Mockito.mock(BigQuery.class));
        writer.setWriteChannelConfig(configuration);

        IllegalArgumentException ex = Assertions.assertThrowsExactly(
                IllegalArgumentException.class, writer::afterPropertiesSet
        );
        Assertions.assertEquals(TestConstants.NO_FORMAT_PROVIDED, ex.getMessage());
    }

    @Test
    void testAfterPropertiesSet_NotValidDataset() {
        WriteChannelConfiguration configuration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, "test"))
                .setSchema(PersonDto.getBigQuerySchema())
                .setFormatOptions(FormatOptions.csv())
                .build();

        BigQueryCsvItemWriter<Object> writer = new BigQueryCsvItemWriter<>();
        writer.setBigQuery(Mockito.mock(BigQuery.class));
        writer.setWriteChannelConfig(configuration);
        writer.setDatasetInfo(DatasetInfo.newBuilder(TestConstants.DATASET + 1).build());

        IllegalArgumentException ex = Assertions.assertThrowsExactly(
                IllegalArgumentException.class, writer::afterPropertiesSet
        );
        Assertions.assertEquals(TestConstants.WRONG_DATASET, ex.getMessage());
    }

    @Test
    void testAfterPropertiesSet_WithoutSchema() {
        WriteChannelConfiguration configuration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, "test"))
                .build();

        BigQueryCsvItemWriter<Object> writer = new BigQueryCsvItemWriter<>();
        writer.setBigQuery(Mockito.mock(BigQuery.class));
        writer.setWriteChannelConfig(configuration);

        IllegalArgumentException ex = Assertions.assertThrowsExactly(
                IllegalArgumentException.class, writer::afterPropertiesSet
        );
        Assertions.assertEquals(TestConstants.NO_SCHEMA_PROVIDED, ex.getMessage());
    }

    @Test
    void testAfterPropertiesSet_NotValidSchema() {
        WriteChannelConfiguration configuration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, "test"))
                .setSchema(PersonDto.getBigQuerySchema())
                .setFormatOptions(FormatOptions.csv())
                .build();

        BigQuery bigQuery = Mockito.mock(BigQuery.class);

        Table table = Mockito.mock(Table.class);
        StandardTableDefinition definition = StandardTableDefinition
                .newBuilder()
                .setSchema(Schema.of(Field.of("test", StandardSQLTypeName.STRING)))
                .build();
        Mockito.when(table.getDefinition()).thenReturn(definition);
        Mockito.when(bigQuery.getTable(configuration.getDestinationTable())).thenReturn(table);

        BigQueryCsvItemWriter<Object> writer = new BigQueryCsvItemWriter<>();
        writer.setBigQuery(bigQuery);
        writer.setWriteChannelConfig(configuration);

        IllegalArgumentException ex = Assertions.assertThrowsExactly(
                IllegalArgumentException.class, writer::afterPropertiesSet
        );
        Assertions.assertEquals(TestConstants.WRONG_SCHEMA, ex.getMessage());
    }

    @Test
    void testWrite_Empty() {
        Assertions.assertDoesNotThrow(() -> new BigQueryCsvItemWriter<>().write(Chunk.of()));
    }

    @Test
    void testWrite() {
        BigQueryItemWriterException ex = Assertions.assertThrowsExactly(
                BigQueryItemWriterException.class, () -> new BigQueryCsvItemWriter<>().write(TestConstants.CHUNK)
        );
        Assertions.assertEquals(TestConstants.WRITE_ERROR, ex.getMessage());
    }

    private static Stream<Arguments> unsupportedFormats() {
        return Stream.of(
                Arguments.of(FormatOptions.bigtable(), TestConstants.BIG_TABLE_NOT_SUPPORTED),
                Arguments.of(FormatOptions.googleSheets(), TestConstants.GOOGLE_SHEETS_NOT_SUPPORTED),
                Arguments.of(FormatOptions.datastoreBackup(), TestConstants.DATASTORE_NOT_SUPPORTED),
                Arguments.of(FormatOptions.parquet(), TestConstants.PARQUET_NOT_SUPPORTED),
                Arguments.of(FormatOptions.orc(), TestConstants.ORC_NOT_SUPPORTED),
                Arguments.of(FormatOptions.avro(), TestConstants.AVRO_NOT_SUPPORTED),
                Arguments.of(FormatOptions.iceberg(), TestConstants.ICEBERG_NOT_SUPPORTED)
        );
    }
}
