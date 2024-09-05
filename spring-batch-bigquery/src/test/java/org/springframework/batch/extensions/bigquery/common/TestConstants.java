/*
 * Copyright 2002-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.extensions.bigquery.common;

import com.google.cloud.bigquery.FieldValueList;
import org.springframework.batch.item.Chunk;
import org.springframework.core.convert.converter.Converter;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public final class TestConstants {

    private TestConstants() {}

    public static final String DATASET = "spring_batch_extensions";
    public static final String NAME = "name";
    public static final String AGE = "age";
    public static final String CSV = "csv";
    public static final String JSON = "json";

    public static final String NO_QUERY_PROVIDED = "No query provided";
    public static final String NO_BIG_QUERY_PROVIDED = "BigQuery service must be provided";
    public static final String NO_ROW_MAPPER_PROVIDED = "Row mapper must be provided";
    public static final String NO_JOB_CONFIGURATION_PROVIDED = "Job configuration must be provided";
    public static final String NO_WRITE_CHANNEL_CONFIGURATION_PROVIDED = "Write channel configuration must be provided";
    public static final String BIG_TABLE_NOT_SUPPORTED = "Google BigTable is not supported";
    public static final String GOOGLE_SHEETS_NOT_SUPPORTED = "Google Sheets is not supported";
    public static final String DATASTORE_NOT_SUPPORTED = "Google Datastore is not supported";
    public static final String PARQUET_NOT_SUPPORTED = "Parquet is not supported";
    public static final String ORC_NOT_SUPPORTED = "Orc is not supported";
    public static final String AVRO_NOT_SUPPORTED = "Avro is not supported";
    public static final String ICEBERG_NOT_SUPPORTED = "Apache Iceberg is not supported";
    public static final String NO_FORMAT_PROVIDED = "Data format must be provided";
    public static final String WRONG_DATASET = "Dataset should be configured properly";
    public static final String NO_SCHEMA_PROVIDED = "Schema must be provided";
    public static final String WRONG_SCHEMA = "Schema must be the same";
    public static final String WRITE_ERROR = "Error on write happened";

    public static final String QUERY = "SELECT p.name, p.age FROM spring_batch_extensions.persons p LIMIT 2";

    public static final Converter<FieldValueList, PersonDto> PERSON_MAPPER = res -> new PersonDto(
            res.get(NAME).getStringValue(), Long.valueOf(res.get(AGE).getLongValue()).intValue()
    );

    /** Order must be defined so later executed queries results could be predictable */
    private static final List<PersonDto> PERSONS = Stream
            .of(new PersonDto("Volodymyr", 27), new PersonDto("Oleksandra", 26))
            .sorted(Comparator.comparing(PersonDto::name))
            .toList();

    public static final Chunk<PersonDto> CHUNK = new Chunk<>(PERSONS);

}