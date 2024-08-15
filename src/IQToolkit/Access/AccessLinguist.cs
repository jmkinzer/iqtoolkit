﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System.Linq.Expressions;

namespace IQToolkit.Access
{
    using Expressions.Sql;
    using Entities;
    using Entities.Mapping;
    using Entities.Translation;
    using Utils;

    internal class AccessLinguist : SqlTranslator
    {
        public AccessLinguist(QueryLanguage language)
            : base(language)
        {
        }

        public override Expression ApplyLanguageRewrites(Expression expression, MappingTranslator mapper, PolicyTranslator police)
        {
            var simplified = expression.SimplifyQueries();

            // fix up any order-by's
            var moved = simplified.MoveOrderByToOuterSelect(this.Language);

            // do default language rewrites
            var rewritten = base.ApplyLanguageRewrites(moved, mapper, police);

            var isolated = rewritten.IsolateCrossJoins();

            var merged = isolated.MergeSubqueries();
            var skipped = merged.ConvertSkipTakeToTop(this.Language);

            var movedAgain = expression = skipped.MoveOrderByToOuterSelect(this.Language);

            var result = movedAgain.SimplifyQueries();

            return result;
        }

        public override FormattedQuery Format(SqlExpression expression, QueryOptions? options = null)
        {
            return AccessFormatter.Singleton.Format(expression, options);
        }

        public override Expression GetGeneratedIdExpression(ColumnMember member)
        {
            return new ScalarFunctionCallExpression(TypeHelper.GetMemberType(member.Member), false, "@@IDENTITY", null);
        }
    }
}