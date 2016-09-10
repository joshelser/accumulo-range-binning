/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.joshelser;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * 
 */
public class YcsbBatchScanner implements Runnable, Closeable, AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(YcsbBatchScanner.class);

  private final Connector conn;
  private final int numRanges;
  private final int numRangesPerPartition;
  private final int threadPoolSize;
  private final ExecutorService svc;
  private final int numIterations;

  public YcsbBatchScanner(Connector conn, int numRanges, int numPartitions, int numIterations) {
    this.conn = Objects.requireNonNull(conn);
    this.numRanges = numRanges;
    this.numRangesPerPartition = numRanges / numPartitions;
    this.threadPoolSize = numPartitions;
    this.svc = Executors.newFixedThreadPool(numPartitions);
    this.numIterations = numIterations;
  }

  public void run() {
    try {
      _run();
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  private void _run() throws Exception {
    log.info("Computing ranges");
    // numRanges
    List<Range> ranges = computeRanges();
    log.info("All ranges calculated: {} ranges found", ranges.size());

    for (int i = 0; i < numIterations; i++) {
      List<List<Range>> partitionedRanges = Lists.partition(ranges, numRangesPerPartition);
      log.info("Executing {} range partitions using a pool of {} threads", partitionedRanges.size(), threadPoolSize);
      List<Future<Integer>> results = new ArrayList<>();
      Stopwatch sw = new Stopwatch();
      sw.start();
      for (List<Range> partition : partitionedRanges) {
//        results.add(this.svc.submit(new BatchScannerQueryTask(conn, partition)));
        results.add(this.svc.submit(new ScannerQueryTask(conn, partition)));
      }
      for (Future<Integer> result : results) {
        log.debug("Found {} results", result.get());
      }
      sw.stop();
      log.info("Queries executed in {} ms", sw.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  private List<Text> computeAllRows() throws Exception {
    ArrayList<Text> rows = new ArrayList<>();
    try (BatchScanner bs = conn.createBatchScanner("usertable", Authorizations.EMPTY, 8)) {
      bs.setRanges(Collections.singleton(new Range()));
      IteratorSetting is = new IteratorSetting(100, FirstEntryInRowIterator.class);
      bs.addScanIterator(is);
      for (Entry<Key,Value> entry : bs) {
        rows.add(entry.getKey().getRow());
      }
      return rows;
    }
  }

  private List<Range> computeRanges() throws Exception {
    List<Text> rows = computeAllRows();
    log.info("Calculated all rows: Found {} rows", rows.size());
    Collections.shuffle(rows);
    log.info("Shuffled all rows");
    LinkedList<Range> ranges = new LinkedList<>();
    Random rand = new Random();
    for (Text row : rows.subList(0, this.numRanges)) {
      // The row, pick a random cq
      ranges.add(Range.exact(row.toString(), "ycsb", "field" + rand.nextInt(10)));
    }
    
    return ranges;
  }

  public static void main(String[] args) throws Exception {
    final Instance inst = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance("accumulo180").withZkHosts("localhost"));
    final Connector conn = inst.getConnector("root", new PasswordToken("secret"));
    log.info("Connected to Accumulo");
    try (YcsbBatchScanner scanner = new YcsbBatchScanner(conn, 3000, 6, 5)) {
      scanner.run();
    }
  }

  @Override
  public void close() throws IOException {
    if (null != svc) {
      svc.shutdown();
      while (!svc.isShutdown()) {
        try {
          svc.awaitTermination(1000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
  
}
