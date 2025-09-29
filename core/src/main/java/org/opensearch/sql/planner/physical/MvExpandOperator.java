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
import java.util.Map;
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
    if (input == null) {
      throw new IllegalArgumentException("MvExpandOperator input cannot be null");
    }
    if (fieldExpr == null) {
      throw new IllegalArgumentException("MvExpandOperator fieldExpr cannot be null");
    }
    this.input = input;
    this.fieldName = fieldExpr.toString();
    System.out.println("DEBUG: MvExpandOperator initialized with fieldName = " + fieldName);
  }

  @Override
  public boolean hasNext() {
    if (expandedIterator.hasNext()) {
      System.out.println("DEBUG: expandedIterator has next");
      return true;
    }
    while (input != null && input.hasNext()) {
      ExprValue inputRow = input.next();
      System.out.println("DEBUG: inputRow = " + inputRow); // Print the tuple/map
      if (inputRow == null) {
        System.out.println("DEBUG: inputRow is null, skipping");
        continue;
      }
      Map<String, ExprValue> tuple = inputRow.tupleValue();
      System.out.println("DEBUG: tupleValue = " + tuple);
      ExprValue ev = tuple.get(fieldName);
      System.out.println("DEBUG: ExprValue for field '" + fieldName + "' = " + ev);

      if (ev == null || ev.isNull() || ev.isMissing()) {
        System.out.println("DEBUG: ExprValue is null or missing, skipping");
        continue;
      }
      Object mvValue = ev.value();
      System.out.println("DEBUG: mvValue (unwrapped) = " + mvValue);

      if (mvValue instanceof List<?>) {
        List<?> mvList = (List<?>) mvValue;
        System.out.println("DEBUG: mvList size = " + mvList.size());
        if (mvList.isEmpty()) {
          System.out.println("DEBUG: mvList is empty, skipping");
          continue;
        }
        List<ExprValue> expandedRows = new ArrayList<>(mvList.size());
        for (Object val : mvList) {
          System.out.println("DEBUG: expanding value = " + val);
          Map<String, Object> newTuple = new HashMap<>();
          for (Map.Entry<String, ExprValue> entry : tuple.entrySet()) {
            Object value = entry.getValue() != null ? entry.getValue().value() : null;
            if (value == null) {
              System.out.println(
                  "DEBUG: field " + entry.getKey() + " is null, skipping from newTuple");
              continue;
            }
            newTuple.put(entry.getKey(), value);
          }
          newTuple.put(fieldName, val);
          System.out.println("DEBUG: newTuple to expand = " + newTuple);
          try {
            ExprValue expanded = ExprValueUtils.tupleValue(newTuple);
            if (expanded == null) {
              System.out.println(
                  "DEBUG: ExprValueUtils.tupleValue returned null for newTuple: "
                      + newTuple
                      + " -- SKIPPING");
              continue; // DO NOT add nulls to expandedRows!
            }
            expandedRows.add(expanded);
          } catch (Exception e) {
            System.out.println(
                "DEBUG: Exception in tupleValue: " + e.getMessage() + " for newTuple: " + newTuple);
            e.printStackTrace();
            continue;
          }
        }
        expandedRows.removeIf(e -> e == null); // Defensive: remove any nulls
        expandedIterator = expandedRows.iterator();
      } else {
        System.out.println("DEBUG: mvValue is single value, expanding = " + mvValue);
        Map<String, Object> newTuple = new HashMap<>();
        for (Map.Entry<String, ExprValue> entry : tuple.entrySet()) {
          Object value = entry.getValue() != null ? entry.getValue().value() : null;
          if (value == null) {
            System.out.println(
                "DEBUG: field " + entry.getKey() + " is null, skipping from newTuple");
            continue;
          }
          newTuple.put(entry.getKey(), value);
        }
        newTuple.put(fieldName, mvValue);
        System.out.println("DEBUG: newTuple to expand = " + newTuple);
        try {
          ExprValue expanded = ExprValueUtils.tupleValue(newTuple);
          if (expanded == null) {
            System.out.println(
                "DEBUG: ExprValueUtils.tupleValue returned null for newTuple: "
                    + newTuple
                    + " -- SKIPPING");
            expandedIterator = Collections.emptyIterator();
          } else {
            expandedIterator = Collections.singletonList(expanded).iterator();
          }
        } catch (Exception e) {
          System.out.println(
              "DEBUG: Exception in tupleValue: " + e.getMessage() + " for newTuple: " + newTuple);
          e.printStackTrace();
          expandedIterator = Collections.emptyIterator();
        }
      }
      if (expandedIterator.hasNext()) {
        System.out.println("DEBUG: expandedIterator is ready, returning true");
        return true;
      } else {
        System.out.println("DEBUG: expandedIterator is empty after expansion");
      }
    }
    System.out.println("DEBUG: hasNext exhausted all input");
    return false;
  }

  @Override
  public ExprValue next() {
    if (!hasNext()) {
      System.out.println(
          "DEBUG: next() called but no more elements, throwing NoSuchElementException");
      throw new NoSuchElementException();
    }
    ExprValue nextValue = expandedIterator.next();
    System.out.println("DEBUG: next() returning value = " + nextValue);
    if (nextValue == null) {
      System.out.println("DEBUG: next() is returning NULL! This should never happen.");
    }
    return nextValue;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    System.out.println("DEBUG: accept() called with visitor = " + visitor);
    return visitor.visitMvExpand(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    System.out.println("DEBUG: getChild() called");
    // Defensive: never return [null]
    if (input == null) {
      System.out.println("DEBUG: getChild() input is null, returning empty list");
      return Collections.emptyList();
    }
    return Collections.singletonList(input);
  }
}
