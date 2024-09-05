package org.springframework.batch.extensions.bigquery.unit.reader;

import com.google.cloud.PageImpl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.reader.BigQueryQueryItemReader;
import org.springframework.batch.extensions.bigquery.unit.base.AbstractBigQueryTest;

import java.util.List;

class BigQueryBatchQueryItemReaderTest extends AbstractBigQueryTest {

    @Test
    void testAfterPropertiesSet_WithoutBigQuery() {
        IllegalArgumentException ex = Assertions.assertThrowsExactly(
                IllegalArgumentException.class, () -> new BigQueryQueryItemReader<>().afterPropertiesSet()
        );
        Assertions.assertEquals(TestConstants.NO_BIG_QUERY_PROVIDED, ex.getMessage());
    }

    @Test
    void testAfterPropertiesSet_WithoutRowMapper() {
        BigQueryQueryItemReader<Object> reader = new BigQueryQueryItemReader<>();
        reader.setBigQuery(Mockito.mock(BigQuery.class));

        IllegalArgumentException ex = Assertions.assertThrowsExactly(IllegalArgumentException.class, reader::afterPropertiesSet);

        Assertions.assertEquals(TestConstants.NO_ROW_MAPPER_PROVIDED, ex.getMessage());
    }

    @Test
    void testAfterPropertiesSet_WithoutJobConfiguration() {
        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReader<>();
        reader.setBigQuery(Mockito.mock(BigQuery.class));
        reader.setRowMapper(TestConstants.PERSON_MAPPER);

        IllegalArgumentException ex = Assertions.assertThrowsExactly(IllegalArgumentException.class, reader::afterPropertiesSet);

        Assertions.assertEquals(TestConstants.NO_JOB_CONFIGURATION_PROVIDED, ex.getMessage());
    }

    @Test
    void testRead() throws Exception {
        PersonDto expected1 = TestConstants.CHUNK.getItems().get(0);
        PersonDto expected2 = TestConstants.CHUNK.getItems().get(1);

        QueryJobConfiguration jobConfiguration = QueryJobConfiguration.of(TestConstants.QUERY);
        BigQuery bigQuery = Mockito.mock(BigQuery.class);
        Mockito.when(bigQuery.query(jobConfiguration)).thenReturn(prepareTableResult(expected1, expected2));

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReader<>();
        reader.setBigQuery(bigQuery);
        reader.setRowMapper(TestConstants.PERSON_MAPPER);
        reader.setJobConfiguration(jobConfiguration);

        PersonDto actual1 = reader.read();
        Assertions.assertEquals(expected1.name(), actual1.name());
        Assertions.assertEquals(expected1.age(), actual1.age());

        PersonDto actual2 = reader.read();
        Assertions.assertEquals(expected2.name(), actual2.name());
        Assertions.assertEquals(expected2.age(), actual2.age());

        Assertions.assertNull(reader.read());
    }

    @Test
    void testRead_Empty() throws Exception {
        TableResult tableResult = TableResult.newBuilder()
                .setTotalRows(0L)
                .setPageNoSchema(new PageImpl<>(null, null, List.of()))
                .build();

        BigQuery bigQuery = Mockito.mock(BigQuery.class);
        QueryJobConfiguration jobConfiguration = QueryJobConfiguration.of(TestConstants.QUERY);
        Mockito.when(bigQuery.query(jobConfiguration)).thenReturn(tableResult);

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReader<>();
        reader.setBigQuery(bigQuery);
        reader.setRowMapper(TestConstants.PERSON_MAPPER);
        reader.setJobConfiguration(jobConfiguration);

        Assertions.assertNull(reader.read());
    }

    private TableResult prepareTableResult(PersonDto person1, PersonDto person2) {
        FieldList fields = FieldList.of(
                Field.of("name", StandardSQLTypeName.STRING),
                Field.of("age", StandardSQLTypeName.STRING)
        );

        List<FieldValue> person1Row = List.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, person1.name()),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, person1.age().toString())
        );

        List<FieldValue> person2Row = List.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, person2.name()),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, person2.age().toString())
        );

        PageImpl<FieldValueList> page = new PageImpl<>(
                null,
                null,
                List.of(FieldValueList.of(person1Row, fields), FieldValueList.of(person2Row, fields))
        );
        return TableResult.newBuilder()
                .setPageNoSchema(page)
                .setTotalRows((long) TestConstants.CHUNK.size())
                .build();
    }

}
