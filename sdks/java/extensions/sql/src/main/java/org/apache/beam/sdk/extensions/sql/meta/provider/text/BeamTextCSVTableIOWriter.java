/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.sql.meta.provider.text;

import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.beamRecord2CsvLine;

import java.io.Serializable;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.BeamRecordType;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.csv.CSVFormat;

/**
 * IOWriter for {@code BeamTextCSVTable}.
 */
public class BeamTextCSVTableIOWriter extends PTransform<PCollection<BeamRecord>, PDone>
    implements Serializable {
  private String filePattern;
  protected BeamRecordType beamRecordType;
  protected CSVFormat csvFormat;

  public BeamTextCSVTableIOWriter(BeamRecordType beamRecordType,
                                  String filePattern,
                                  CSVFormat csvFormat) {
    this.filePattern = filePattern;
    this.beamRecordType = beamRecordType;
    this.csvFormat = csvFormat;
  }

  @Override
  public PDone expand(PCollection<BeamRecord> input) {
    return input.apply("encodeRecord", ParDo.of(new DoFn<BeamRecord, String>() {

      @ProcessElement
      public void processElement(ProcessContext ctx) {
        BeamRecord row = ctx.element();
        ctx.output(beamRecord2CsvLine(row, csvFormat));
      }
    })).apply(TextIO.write().to(filePattern));
  }
}
