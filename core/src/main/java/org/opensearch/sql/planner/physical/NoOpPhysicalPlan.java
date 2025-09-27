/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.opensearch.sql.data.model.ExprValue;

/** NoOpPhysicalPlan produces no results. */
public class NoOpPhysicalPlan extends PhysicalPlan {
  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public ExprValue next() {
    throw new NoSuchElementException();
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.emptyList();
  }
}
