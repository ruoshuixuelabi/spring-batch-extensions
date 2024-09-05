package org.springframework.batch.extensions.bigquery.example.writer.builder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.example.base.AbstractExampleTest;
import org.springframework.batch.extensions.bigquery.writer.BigQueryCsvItemWriter;
import org.springframework.batch.extensions.bigquery.writer.builder.BigQueryCsvItemWriterBuilder;

class BigQueryCsvItemWriterBuilderTest extends AbstractExampleTest {

    private static final String TABLE = "persons_csv";

    private final Log logger = LogFactory.getLog(getClass());

    /**
     * Example how CSV writer is expected to be built without {@link org.springframework.context.annotation.Bean} annotation.
     */
    @Test
    void testCsvWriterWithRowMapper() {
        BigQuery mockedBigQuery = prepareMockedBigQuery();
        CsvMapper csvMapper = new CsvMapper();
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(TestConstants.DATASET).setLocation("europe-west-2").build();

        WriteChannelConfiguration writeConfiguration = WriteChannelConfiguration
                .newBuilder(TableId.of(datasetInfo.getDatasetId().getDataset(), TABLE))
                .setAutodetect(true)
                .setFormatOptions(FormatOptions.csv())
                .build();

        BigQueryCsvItemWriter<PersonDto> writer = new BigQueryCsvItemWriterBuilder<PersonDto>()
                .bigQuery(mockedBigQuery)
                .rowMapper(dto -> convertDtoToCsvByteArray(csvMapper, dto))
                .writeChannelConfig(writeConfiguration)
                .datasetInfo(datasetInfo)
                .jobConsumer(job -> this.logger.debug("Job with id: " + job.getJobId() + " is created"))
                .build();

        writer.afterPropertiesSet();

        Assertions.assertNotNull(writer);
    }

    @Test
    void testCsvWriterWithCsvMapper() {
        BigQuery mockedBigQuery = prepareMockedBigQuery();

        WriteChannelConfiguration writeConfiguration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, TABLE))
                .setAutodetect(true)
                .setFormatOptions(FormatOptions.csv())
                .build();

        BigQueryCsvItemWriter<PersonDto> writer = new BigQueryCsvItemWriterBuilder<PersonDto>()
                .bigQuery(mockedBigQuery)
                .writeChannelConfig(writeConfiguration)
                .build();

        writer.afterPropertiesSet();

        Assertions.assertNotNull(writer);
    }

    private byte[] convertDtoToCsvByteArray(CsvMapper csvMapper, PersonDto dto) {
        try {
            return csvMapper.writerWithSchemaFor(PersonDto.class).writeValueAsBytes(dto);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
