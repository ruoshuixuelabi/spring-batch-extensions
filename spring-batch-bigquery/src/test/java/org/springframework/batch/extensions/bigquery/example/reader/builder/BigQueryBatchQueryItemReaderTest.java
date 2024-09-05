package org.springframework.batch.extensions.bigquery.example.reader.builder;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.example.base.AbstractExampleTest;
import org.springframework.batch.extensions.bigquery.reader.BigQueryQueryItemReader;
import org.springframework.batch.extensions.bigquery.reader.builder.BigQueryQueryItemReaderBuilder;

class BigQueryBatchQueryItemReaderTest extends AbstractExampleTest {

    @Test
    void testCustomReader() {
        QueryJobConfiguration jobConfiguration = QueryJobConfiguration
                .newBuilder(TestConstants.QUERY)
                .setDestinationTable(TableId.of(TestConstants.DATASET, "persons_duplicate"))
                .setPriority(QueryJobConfiguration.Priority.BATCH)
                .build();

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReaderBuilder<PersonDto>()
                .bigQuery(prepareMockedBigQuery())
                .jobConfiguration(jobConfiguration)
                .rowMapper(TestConstants.PERSON_MAPPER)
                .build();

        reader.afterPropertiesSet();

        Assertions.assertNotNull(reader);
    }

}
