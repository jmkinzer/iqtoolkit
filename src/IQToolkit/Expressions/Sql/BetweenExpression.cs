﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System.Linq.Expressions;

namespace IQToolkit.Expressions.Sql
{
    /// <summary>
    /// The between expression: expr between (lower, upper)
    /// </summary>
    public sealed class BetweenExpression : SqlExpression
    {
        /// <summary>
        /// The expression to test.
        /// </summary>
        public Expression Expression { get; }

        /// <summary>
        /// The lower bound of the between operator.
        /// </summary>
        public Expression Lower { get; }

        /// <summary>
        /// The upper bound of the between operator.
        /// </summary>
        public Expression Upper { get; }

        public BetweenExpression(
            Expression expression, 
            Expression lower, 
            Expression upper)
            : base(expression.Type)
        {
            this.Expression = expression;
            this.Lower = lower;
            this.Upper = upper;
        }

        public override bool IsPredicate => true;

        public BetweenExpression Update(
            Expression expression, 
            Expression lower, 
            Expression upper)
        {
            if (expression != this.Expression 
                || lower != this.Lower 
                || upper != this.Upper)
            {
                return new BetweenExpression(expression, lower, upper);
            }
            else
            {
                return this;
            }
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            if (visitor is SqlExpressionVisitor dbVisitor)
                return dbVisitor.VisitBetween(this);
            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var expr = visitor.Visit(this.Expression);
            var lower = visitor.Visit(this.Lower);
            var upper = visitor.Visit(this.Upper);
            return Update(expr, lower, upper);
        }
    }
}
