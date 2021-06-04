package com.streaming;

import java.util.*;
import java.util.ArrayList;

import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.GroupByKey;

public class aggregate {
  static class GroupByFlight extends DoFn<TableRow, KV<String, TableRow>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row = c.element();
      String key = (String) row.get("iataId");
      c.output(KV.of(key, row));
    }
  } 

  static class tableRowMax extends DoFn<KV<String, Iterable<TableRow>>, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String lookUpKey = c.element().getKey();
      Iterable<TableRow> rows = c.element().getValue();
      
      ArrayList<Integer> speedList = new ArrayList<Integer>();
      TableRow tempRow = new TableRow();
      for (TableRow i : rows) {
        int speed = Integer.parseInt(i.get("speed").toString());
        speedList.add(speed);
        tempRow = i;
      }
      TableRow outputRow = tempRow;
      Double avgSpeed = speedList.stream().mapToInt(Integer::intValue).average().getAsDouble();
      outputRow.set("speed", avgSpeed);
      c.output(outputRow);
    }
  }

  static class MaxMeanTemp extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
    @Override
    public PCollection<TableRow> expand(PCollection<TableRow> rows) {

      PCollection<KV<String, TableRow>> messageWithKey = rows.apply("Convert PubSub TableRows to Key/Value Pairs", ParDo.of( new GroupByFlight() ));
      PCollection<KV<String, Iterable<TableRow>>> messageWithKeyList = messageWithKey.apply(GroupByKey.<String, TableRow>create());
      PCollection<TableRow> results = messageWithKeyList.apply(ParDo.of(new tableRowMax()));

      return results;
    }
  }
}