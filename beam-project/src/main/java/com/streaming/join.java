package com.streaming;

import java.util.Map;

import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.KV;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.View;


public class join {
    static class GroupByAircraft extends DoFn<TableRow, KV<String, TableRow>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            String key = (String) row.get("type");
            c.output(KV.of(key, row));
        }
    }

    static PCollection<TableRow> joinRows(PCollection<TableRow> convertedTableRows, PCollection<TableRow> dimensions) {

        PCollection<KV<String, TableRow>> sideInput = dimensions.apply("Convert Dim Table TableRows to Key/Value Pairs", ParDo.of( new GroupByAircraft() ));
        PCollection<KV<String, TableRow>> messageWithKey = convertedTableRows.apply("Convert PubSub TableRows to Key/Value Pairs", ParDo.of( new GroupByAircraft() ));

        PCollectionView<Map<String, Iterable<TableRow>>> view = sideInput.apply("Dim Table as SideInput", View.<String, TableRow>asMultimap());

        PCollection<TableRow> joinedTableRows = messageWithKey.apply(
                                    "Join PubSub Messages with BigQuery Table", 
                                    ParDo.of(new DoFn<KV<String, TableRow>, TableRow>() {
                                        @ProcessElement
                                        public void processElement(ProcessContext c) {
                                            String lookUpKey = c.element().getKey();
                                            Iterable<TableRow> sideInputRow = c.sideInput(view).get(lookUpKey);
                                            TableRow mainInputRow = c.element().getValue();
                                            if (sideInputRow != null) {
                                                for (TableRow entry : sideInputRow) {
                                                    TableRow row = new TableRow();
                                                    row = mainInputRow;
                                                    row.putAll(entry);
                                                    c.output(row);
                                                }
                                            } else {
                                                TableRow row = new TableRow();
                                                row = mainInputRow;
                                                c.output(row);
                                            }
                                        }
                                    }).withSideInputs(view)); 
        return joinedTableRows;
    }
}