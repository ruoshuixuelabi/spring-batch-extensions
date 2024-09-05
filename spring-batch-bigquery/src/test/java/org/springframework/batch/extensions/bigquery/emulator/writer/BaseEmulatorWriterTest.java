package org.springframework.batch.extensions.bigquery.emulator.writer;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.junit.jupiter.api.Assertions;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.emulator.base.AbstractEmulatorTest;
import org.springframework.batch.extensions.bigquery.writer.BigQueryJsonItemWriter;
import org.springframework.batch.extensions.bigquery.writer.builder.BigQueryJsonItemWriterBuilder;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

abstract class BaseEmulatorWriterTest extends AbstractEmulatorTest {

    void performTest(WriteChannelConfiguration config, TableId tableId) throws Exception {
        AtomicReference<Job> job = new AtomicReference<>();

        BigQueryJsonItemWriter<PersonDto> writer = new BigQueryJsonItemWriterBuilder<PersonDto>()
                .bigQuery(bigQuery)
                .writeChannelConfig(config)
                .jobConsumer(job::set)
                .build();

        writer.afterPropertiesSet();
        writer.write(TestConstants.CHUNK);
        Job actualJob = job.get().waitFor();

        List<FieldValueList> result = StreamSupport
                .stream(bigQuery.listTableData(tableId, BigQuery.TableDataListOption.pageSize(5L)).getValues().spliterator(), false)
                .toList();

        JobId jobId = actualJob.getJobId();
        Assertions.assertEquals(AbstractEmulatorTest.PROJECT, jobId.getProject());
        Assertions.assertNotNull(jobId.getJob());
        Assertions.assertEquals(JobStatus.State.DONE, actualJob.getStatus().getState());
        Assertions.assertEquals(JobConfiguration.Type.LOAD, actualJob.getConfiguration().getType());

        FieldValueList bqRow1 = result.get(0);
        PersonDto expected1 = TestConstants.CHUNK.getItems().get(0);
        Assertions.assertEquals(expected1.name(), bqRow1.get(0).getStringValue());
        Assertions.assertEquals(expected1.age(), bqRow1.get(1).getNumericValue().intValue());

        FieldValueList bqRow2 = result.get(1);
        PersonDto expected2 = TestConstants.CHUNK.getItems().get(1);
        Assertions.assertEquals(expected2.name(), bqRow2.get(0).getStringValue());
        Assertions.assertEquals(expected2.age(), bqRow2.get(1).getNumericValue().intValue());
    }
}
