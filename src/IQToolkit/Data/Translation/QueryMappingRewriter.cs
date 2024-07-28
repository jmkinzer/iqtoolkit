﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data.Translation
{
    using Expressions;
    using Mapping;

    /// <summary>
    /// Applies mapping rules to queries.
    /// </summary>
    public abstract class QueryMappingRewriter
    {
        /// <summary>
        /// The related <see cref="QueryTranslator"/>
        /// </summary>
        public abstract QueryTranslator Translator { get; }

        /// <summary>
        /// The mapping to apply.
        /// </summary>
        public abstract QueryMapping Mapping { get; }

        /// <summary>
        /// Get a query expression that selects all entities from a table
        /// </summary>
        public abstract ClientProjectionExpression GetQueryExpression(MappingEntity entity);

        /// <summary>
        /// Gets an expression that constructs an entity instance relative to a root.
        /// The root is most often a TableExpression, but may be any other experssion such as
        /// a ConstantExpression.
        /// </summary>
        public abstract EntityExpression GetEntityExpression(Expression root, MappingEntity entity);

        /// <summary>
        /// Get an expression for a mapped property relative to a root expression. 
        /// The root is either a TableExpression or an expression defining an entity instance.
        /// </summary>
        public abstract Expression GetMemberExpression(Expression root, MappingEntity entity, MemberInfo member);

        /// <summary>
        /// Get an expression that represents the insert operation for the specified instance.
        /// </summary>
        /// <param name="entity">The mapping for the entity.</param>
        /// <param name="instance">The instance to insert.</param>
        /// <param name="selector">A lambda expression that computes a return value from the operation.</param>
        /// <returns></returns>
        public abstract Expression GetInsertExpression(MappingEntity entity, Expression instance, LambdaExpression? selector);

        /// <summary>
        /// Get an expression that represents the update operation for the specified instance.
        /// </summary>
        public abstract Expression GetUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression? updateCheck, LambdaExpression? selector, Expression? @else);

        /// <summary>
        /// Get an expression that represents the insert-or-update operation for the specified instance.
        /// </summary>
        public abstract Expression GetInsertOrUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression? updateCheck, LambdaExpression? resultSelector);

        /// <summary>
        /// Get an expression that represents the delete operation for the specified instance.
        /// </summary>
        public abstract Expression GetDeleteExpression(MappingEntity entity, Expression? instance, LambdaExpression? deleteCheck);

        /// <summary>
        /// Recreate the type projection with the additional members included
        /// </summary>
        public abstract EntityExpression IncludeMembers(EntityExpression entity, Func<MemberInfo, bool> fnIsIncluded);

        /// <summary>
        /// Return true if the entity expression has included members.
        /// </summary>
        public abstract bool HasIncludedMembers(EntityExpression entity);

        /// <summary>
        /// Apply mapping to a sub query expression
        /// </summary>
        public virtual Expression ApplyMapping(Expression expression)
        {
            return expression.ConvertQueryOperatorsToDbExpressions(this.Translator.Language, this, isQueryFragment: true);
        }

        /// <summary>
        /// Rewrites queries with regards to mapping rules.
        /// </summary>
        public virtual Expression Rewrite(Expression expression)
        {
            // convert references to LINQ operators into query specific DbExpression's
            var bound = expression.ConvertQueryOperatorsToDbExpressions(this.Translator.Language, this);

            // move aggregate computations so they occur in same select as group-by
            //var aggmoved = bound.MoveAggregateSubqueries(this.Translator.Language);

            // convert references to association properties into correlated queries
            var related = bound.ConvertRelationshipAccesses(this);

            // rewrite comparision checks between entities and multi-valued constructs
            var result = related.ConvertEntityComparisons(this.Mapping);

            return result;
        }
    }
}