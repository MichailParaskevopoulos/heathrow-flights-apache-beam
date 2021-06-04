package com.streaming;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;

import java.nio.charset.StandardCharsets;

import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.coders.Coder.Context;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.PTransform;

import org.apache.beam.sdk.values.PCollection;

public class messageParsing {
    public static TableRow convertJsonToTableRow(String json) {
        TableRow row;

        try {
            InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)); 
            row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }

        return row;
    }

    public static class JsonToTableRow extends PTransform<PCollection<String>, PCollection<TableRow>> {

        @Override
        public PCollection<TableRow> expand(PCollection<String> stringPCollection) {
            return stringPCollection.apply(
                "Convert JSON to TableRow",
                MapElements.via(
                    new SimpleFunction<String, TableRow>() {
                    @Override
                    public TableRow apply(String json) {
                    return convertJsonToTableRow(json);
                }
            }));
        }
    }
}