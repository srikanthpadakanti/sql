/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Iterator;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.planner.PlanNode;
import org.opensearch.sql.storage.split.Split;

/** Physical plan. */
public abstract class PhysicalPlan
    implements PlanNode<PhysicalPlan>, Iterator<ExprValue>, AutoCloseable {
  /**
   * Accept the {@link PhysicalPlanNodeVisitor}.
   *
   * @param visitor visitor.
   * @param context visitor context.
   * @param <R> returned object type.
   * @param <C> context type.
   * @return returned object.
   */
  public abstract <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context);

  //  public void open() {
  //    getChild().forEach(PhysicalPlan::open);
  //  }

  //  public void close() {
  //    getChild().forEach(PhysicalPlan::close);
  //  }

  //  public void add(Split split) {
  //    getChild().forEach(child -> child.add(split));
  //  }

  public void open() {
    System.out.println("DEBUG: PhysicalPlan.open() called for class: " + this.getClass().getName());
    for (PhysicalPlan child : getChild()) {
      if (child == null) {
        System.out.println(
            "DEBUG: PhysicalPlan.open() found child == null in " + this.getClass().getName());
        continue; // or throw new IllegalStateException("Null child in PhysicalPlan.open()");
      }
      System.out.println("DEBUG: PhysicalPlan.open() opening child: " + child.getClass().getName());
      child.open();
    }
  }

  public void close() {
    System.out.println(
        "DEBUG: PhysicalPlan.close() called for class: " + this.getClass().getName());
    for (PhysicalPlan child : getChild()) {
      if (child == null) {
        System.out.println(
            "DEBUG: PhysicalPlan.close() found child == null in " + this.getClass().getName());
        continue;
      }
      System.out.println(
          "DEBUG: PhysicalPlan.close() closing child: " + child.getClass().getName());
      child.close();
    }
  }

  public void add(Split split) {
    System.out.println(
        "DEBUG: PhysicalPlan.add() called for class: "
            + this.getClass().getName()
            + " with split: "
            + split);
    for (PhysicalPlan child : getChild()) {
      if (child == null) {
        System.out.println(
            "DEBUG: PhysicalPlan.add() found child == null in " + this.getClass().getName());
        continue;
      }
      System.out.println(
          "DEBUG: PhysicalPlan.add() adding split to child: " + child.getClass().getName());
      child.add(split);
    }
  }

  public ExecutionEngine.Schema schema() {
    throw new IllegalStateException(
        String.format(
            "[BUG] schema can been only applied to " + "ProjectOperator, instead of %s",
            this.getClass().getSimpleName()));
  }
}
