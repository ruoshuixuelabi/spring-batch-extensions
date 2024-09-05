package org.springframework.batch.extensions.bigquery.example.base;

import com.google.cloud.bigquery.BigQuery;
import org.mockito.Mockito;

public abstract class AbstractExampleTest {

    protected BigQuery prepareMockedBigQuery() {
        BigQuery mockedBigQuery = Mockito.mock(BigQuery.class);

        Mockito
                .when(mockedBigQuery.getTable(Mockito.any()))
                .thenReturn(null);

        Mockito
                .when(mockedBigQuery.getDataset(Mockito.anyString()))
                .thenReturn(null);

        return mockedBigQuery;
    }

}
