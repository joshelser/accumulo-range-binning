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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import com.google.common.collect.Iterables;

/**
 * 
 */
public class ScannerQueryTask implements Callable<Integer> {

  private final Connector conn;
  private final List<Range> ranges;

  public ScannerQueryTask(Connector conn, List<Range> ranges) {
    this.conn = Objects.requireNonNull(conn);
    this.ranges = Objects.requireNonNull(ranges);
  }

  @Override
  public Integer call() throws Exception {
    int count = 0;
    for (Range r : ranges) {
      try (Scanner s = conn.createScanner("usertable", Authorizations.EMPTY)) {
        s.setRange(r);
        count += Iterables.size(s);
      }
    }
    return count;
  }
}
