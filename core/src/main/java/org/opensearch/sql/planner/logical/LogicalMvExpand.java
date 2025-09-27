/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Logical plan node for mvexpand command. Represents an operation that expands multivalue fields
 * into separate rows.
 *
 * <p>Provides both the {@link Field} object and an {@link Expression} for the referenced field,
 * allowing downstream physical operators to access the field with type safety and compatibility.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalMvExpand extends LogicalPlan {

  /** The field to be expanded (multi-value array/collection). */
  private final Field field;

  /** Reference to the field as an Expression, for use in physical layer. */
  private final Expression fieldExpr;

  /**
   * Creates a LogicalMvExpand node.
   *
   * @param child The input logical plan to which mvexpand will be applied.
   * @param field The field to expand.
   */
  public LogicalMvExpand(LogicalPlan child, Field field) {
    super(Collections.singletonList(child));
    if (child == null) throw new IllegalArgumentException("MvExpand child cannot be null");
    if (field == null) throw new IllegalArgumentException("MvExpand field cannot be null");
    this.field = field;
    this.fieldExpr = new ReferenceExpression(field.getField().toString(), UNKNOWN);
  }

  /**
   * Implements the visitor pattern for LogicalPlan traversal.
   *
   * @param visitor Logical plan node visitor.
   * @param context Arbitrary context object.
   * @param <R> Return type.
   * @param <C> Context type.
   * @return Visitor return value.
   */
  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitMvExpand(this, context);
  }
}
