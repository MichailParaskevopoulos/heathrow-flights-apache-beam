package com.streaming;

import com.streaming.messageParsing.JsonToTableRow;
import com.streaming.join;
import com.streaming.aggregate.MaxMeanTemp;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;

import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;

import org.joda.time.Duration;

import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;

public class ETL {
  // configuration options for Dataflow runner
  public interface BeamOptions extends PipelineOptions, StreamingOptions {
    @Description("The Cloud Pub/Sub topic to read from")
    @Required
    String getInputTopic();

    void setInputTopic(String value);

    @Description("Window Size")
    @Default.Integer(1)
    Integer getWindowSize();

    void setWindowSize(Integer value);

  }

  public static void main(String[] args) throws IOException {
    BeamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BeamOptions.class);
    Integer windowSize = options.getWindowSize();
    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<TableRow> dimensionalRows = null;
    dimensionalRows = pipeline.apply("Read Dimensional Table from BigQuery",
                                BigQueryIO.readTableRows()
                                .from(String.format("%s:%s.%s", "covid19flights", "covid19_airtraffic", "aircraft_typesV2")));

    PCollection<String> messages = null;
    messages = pipeline.apply("Pull PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                       .apply(Window.into(FixedWindows.of(Duration.standardMinutes(windowSize))));

    PCollection<TableRow> convertedTableRows = messages.apply("Convert PubSub messages to TableRow Type", new JsonToTableRow());
    
    PCollection<TableRow> maxTableRows = convertedTableRows.apply(new MaxMeanTemp());

    PCollection<TableRow> rowsWithTime = maxTableRows.apply("Set timestamp for aggregated message", ParDo.of(new DoFn<TableRow, TableRow> () {
                                                                                                      @ProcessElement
                                                                                                      public void processElement(ProcessContext c) {
                                                                                                        TableRow row = c.element();
                                                                                                        row.set("timestamp", c.timestamp().toString());
                                                                                                        c.output(row);
                                                                                                      }
                                                                                                  }));

    PCollection<TableRow> finalTableRows = join.joinRows(rowsWithTime, dimensionalRows); // perform join between aggregated message and dimensional table
    
    finalTableRows.apply(
            "Write Processed Results to BigQuery",
            BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.%s", "covid19flights", "covid19_airtraffic", "flight_streamV3"))
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));
        
    pipeline.run().waitUntilFinish();
  }
}