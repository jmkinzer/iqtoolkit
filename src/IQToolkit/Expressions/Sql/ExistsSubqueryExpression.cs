﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System.Linq.Expressions;

namespace IQToolkit.Expressions.Sql
{
    /// <summary>
    /// A SQL EXISTS subquery.
    /// </summary>
    public sealed class ExistsSubqueryExpression : SubqueryExpression
    {
        public new SelectExpression Select => base.Select!;

        public ExistsSubqueryExpression(SelectExpression select)
            : base(typeof(bool), select)
        {
        }

        public override bool IsPredicate => true;

        public ExistsSubqueryExpression Update(
            SelectExpression select)
        {
            if (select != this.Select)
            {
                return new ExistsSubqueryExpression(select);
            }
            else
            {
                return this;
            }
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            if (visitor is SqlExpressionVisitor dbVisitor)
                return dbVisitor.VisitExistsSubquery(this);
            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var select = (SelectExpression)visitor.Visit(this.Select);
            return this.Update(select);
        }
    }
}
