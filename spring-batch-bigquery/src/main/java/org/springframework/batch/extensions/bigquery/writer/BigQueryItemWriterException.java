package org.springframework.batch.extensions.bigquery.writer;

import org.springframework.batch.item.ItemWriterException;

public class BigQueryItemWriterException extends ItemWriterException {
    public BigQueryItemWriterException(String message, Throwable cause) {
        super(message, cause);
    }
}
