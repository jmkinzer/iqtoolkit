﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Linq.Expressions;

namespace IQToolkit.Expressions.Sql
{
    /// <summary>
    /// An aggregate expression used in select column.
    /// </summary>
    public sealed class AggregateExpression : SqlExpression
    {
        public string AggregateName { get; }
        public Expression? Argument { get; }
        public bool IsDistinct { get; }

        public AggregateExpression(Type type, string aggregateName, Expression? argument, bool isDistinct)
            : base(type)
        {
            this.AggregateName = aggregateName;
            this.Argument = argument;
            this.IsDistinct = isDistinct;
        }

        public AggregateExpression Update(
            Type type,
            string aggType,
            Expression? arg,
            bool isDistinct)
        {
            if (type != this.Type
                || aggType != this.AggregateName
                || arg != this.Argument
                || isDistinct != this.IsDistinct)
            {
                return new AggregateExpression(type, aggType, arg, isDistinct);
            }
            else
            {
                return this;
            }
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            if (visitor is SqlExpressionVisitor dbVisitor)
                dbVisitor.VisitAggregate(this);
            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var argument = visitor.Visit(this.Argument);
            return this.Update(this.Type, this.AggregateName, argument, this.IsDistinct);
        }
    }
}
