/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Physical operator for the mvexpand command. Expands multi-value fields into separate rows,
 * handling nulls and empty arrays gracefully.
 */
public class MvExpandOperator extends PhysicalPlan {

  private final PhysicalPlan input;
  private final String fieldName;
  private Iterator<ExprValue> expandedIterator = Collections.emptyIterator();

  public MvExpandOperator(PhysicalPlan input, ReferenceExpression fieldExpr) {
    super();
    this.input = input;
    if (fieldExpr == null) {
      throw new IllegalArgumentException("MvExpandOperator fieldExpr cannot be null");
    }
    // Use the field name as string, same as other operators
    this.fieldName = fieldExpr.toString();
  }

  @Override
  public boolean hasNext() {
    if (expandedIterator.hasNext()) {
      return true;
    }
    while (input != null && input.hasNext()) {
      ExprValue inputRow = input.next();
      System.out.println("DEBUG: inputRow = " + inputRow); // Print the tuple/map
      if (inputRow == null) {
        continue;
      }
      Object mvValue = inputRow.tupleValue().get(fieldName);
      // Skip if field missing or null
      if (mvValue == null) {
        continue;
      }
      if (mvValue instanceof List<?>) {
        List<?> mvList = (List<?>) mvValue;
        if (mvList.isEmpty()) {
          continue;
        }
        List<ExprValue> expandedRows = new ArrayList<>(mvList.size());
        for (Object val : mvList) {
          HashMap<String, Object> newTuple = new HashMap<>(inputRow.tupleValue());
          newTuple.put(fieldName, val);
          expandedRows.add(ExprValueUtils.tupleValue(newTuple));
        }
        expandedIterator = expandedRows.iterator();
      } else {
        HashMap<String, Object> newTuple = new HashMap<>(inputRow.tupleValue());
        newTuple.put(fieldName, mvValue);
        expandedIterator =
            Collections.singletonList(ExprValueUtils.tupleValue(newTuple)).iterator();
      }
      if (expandedIterator.hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public ExprValue next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return expandedIterator.next();
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return input == null ? Collections.emptyList() : Collections.singletonList(input);
  }
}
