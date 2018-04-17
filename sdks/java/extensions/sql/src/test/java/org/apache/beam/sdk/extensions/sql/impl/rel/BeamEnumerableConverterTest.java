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

package org.apache.beam.sdk.extensions.sql.impl.rel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamIOType;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.junit.Test;

/**
 * Test for {@code BeamEnumerableConverter}.
 */
public class BeamEnumerableConverterTest {
  static RexBuilder rexBuilder = new RexBuilder(BeamQueryPlanner.TYPE_FACTORY);

  @Test
  public void testToEnumerable_collectSingle() {
    Schema schema = Schema.builder().addInt64Field("id", false).build();
    RelDataType type = CalciteUtils.toCalciteRowType(schema, BeamQueryPlanner.TYPE_FACTORY);
    ImmutableList<ImmutableList<RexLiteral>> tuples =
        ImmutableList.of(ImmutableList.of(rexBuilder.makeBigintLiteral(BigDecimal.ZERO)));
    BeamRelNode node = new BeamValuesRel(null, type, tuples, null);

    Enumerable<Object> enumerable = BeamEnumerableConverter.toEnumerable(node);
    Enumerator<Object> enumerator = enumerable.enumerator();

    assertTrue(enumerator.moveNext());
    assertEquals(enumerator.current(), 0L);
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  @Test
  public void testToEnumerable_collectMultiple() {
    Schema schema =
        Schema.builder().addInt64Field("id", false).addInt64Field("otherid", false).build();
    RelDataType type = CalciteUtils.toCalciteRowType(schema, BeamQueryPlanner.TYPE_FACTORY);
    ImmutableList<ImmutableList<RexLiteral>> tuples =
        ImmutableList.of(
            ImmutableList.of(
                rexBuilder.makeBigintLiteral(BigDecimal.ZERO),
                rexBuilder.makeBigintLiteral(BigDecimal.ONE)));
    BeamRelNode node = new BeamValuesRel(null, type, tuples, null);

    Enumerable<Object> enumerable = BeamEnumerableConverter.toEnumerable(node);
    Enumerator<Object> enumerator = enumerable.enumerator();

    assertTrue(enumerator.moveNext());
    Object[] row = (Object[]) enumerator.current();
    assertEquals(row.length, 2);
    assertEquals(row[0], 0L);
    assertEquals(row[1], 1L);
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  private class MockTable implements BeamSqlTable {
    public BeamIOType getSourceType() {
      return null;
    }

    public PCollection<Row> buildIOReader(Pipeline pipeline) {
      return null;
    }

    public PTransform<? super PCollection<Row>, POutput> buildIOWriter() {
      return new PTransform<PCollection<Row>, POutput>() {
        @Override
        public POutput expand(PCollection<Row> input) {
          return PDone.in(input.getPipeline());
        }
      };
    }

    public Schema getSchema() {
      return null;
    }
  }
}
