﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Entities.Translation
{
    using Collections;
    using Utils;

    public static class Aggregator
    {
        /// <summary>
        /// Get a function that coerces a sequence of one type into another type.
        /// This is primarily used for aggregators stored in ProjectionExpression's, which are used to represent the 
        /// final transformation of the entire result set of a query.
        /// </summary>
        public static LambdaExpression? GetAggregator(Type expectedType, Type actualType)
        {
            Type actualElementType = TypeHelper.GetSequenceElementType(actualType);
            if (!expectedType.IsAssignableFrom(actualType))
            {
                var expectedElementType = TypeHelper.GetSequenceElementType(expectedType);
                var p = Expression.Parameter(actualType, "p");
                Expression? body = null;

                if (expectedType.IsAssignableFrom(actualElementType))
                {
                    body = Expression.Call(typeof(Enumerable), "SingleOrDefault", new Type[] { actualElementType }, p);
                }
                else if (expectedType.IsGenericType 
                    && (expectedType == typeof(IQueryable) 
                        || expectedType == typeof(IOrderedQueryable) 
                        || expectedType.GetGenericTypeDefinition() == typeof(IQueryable<>) 
                        || expectedType.GetGenericTypeDefinition() == typeof(IOrderedQueryable<>)
                        ))
                {
                    body = Expression.Call(typeof(Queryable), "AsQueryable", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));
                    if (body.Type != expectedType)
                    {
                        body = Expression.Convert(body, expectedType);
                    }
                }
                else if (expectedType.IsArray && expectedType.GetArrayRank() == 1)
                {
                    body = Expression.Call(typeof(Enumerable), "ToArray", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));
                }
                else if (expectedType.IsGenericType && expectedType.GetGenericTypeDefinition().IsAssignableFrom(typeof(IList<>)))
                {
                    var gt = typeof(DeferredList<>).MakeGenericType(expectedType.GenericTypeArguments);
                    gt.TryGetDeclaredConstructor(
                        new Type[] {typeof(IEnumerable<>).MakeGenericType(expectedType.GenericTypeArguments)},
                        out var cn
                        );
                    body = Expression.New(cn, CoerceElement(expectedElementType, p));
                }
                else if (expectedType.IsAssignableFrom(typeof(List<>).MakeGenericType(actualElementType)))
                {
                    // List<T> can be assigned to expectedType
                    body = Expression.Call(typeof(Enumerable), "ToList", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));
                }
                else
                {
                    // some other collection type that has a constructor that takes IEnumerable<T>
                    if (expectedType.TryGetDeclaredConstructor(new Type[] { actualType }, out var ci))
                    {
                        body = Expression.New(ci, p);
                    }
                }

                if (body != null)
                {
                    return Expression.Lambda(body, p);
                }
            }

            return null;
        }

        private static Expression CoerceElement(Type expectedElementType, Expression expression)
        {
            Type elementType = TypeHelper.GetSequenceElementType(expression.Type);
            if (expectedElementType != elementType 
                && (expectedElementType.IsAssignableFrom(elementType) 
                    || elementType.IsAssignableFrom(expectedElementType)))
            {
                return Expression.Call(typeof(Enumerable), "Cast", new Type[] { expectedElementType }, expression);
            }
            return expression;
        }
    }
}