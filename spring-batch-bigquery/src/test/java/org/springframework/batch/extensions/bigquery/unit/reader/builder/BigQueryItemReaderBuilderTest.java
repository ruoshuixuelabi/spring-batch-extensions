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

package org.springframework.batch.extensions.bigquery.unit.reader.builder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.reader.BigQueryQueryItemReader;
import org.springframework.batch.extensions.bigquery.reader.builder.BigQueryQueryItemReaderBuilder;
import org.springframework.batch.extensions.bigquery.unit.base.AbstractBigQueryTest;

class BigQueryItemReaderBuilderTest extends AbstractBigQueryTest {

    @Test
    void testBuild_WithoutJobConfiguration_WithoutQuery() {
        IllegalArgumentException ex = Assertions.assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new BigQueryQueryItemReaderBuilder<PersonDto>().build()
        );

        Assertions.assertEquals(TestConstants.NO_QUERY_PROVIDED, ex.getMessage());
    }

    @Test
    void testBuild_WithoutJobConfiguration_WithQuery() {
        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReaderBuilder<PersonDto>()
                .query("SELECT p.name, p.age FROM spring_batch_extensions.persons p LIMIT 2")
                .build();
        Assertions.assertNotNull(reader);
    }

}