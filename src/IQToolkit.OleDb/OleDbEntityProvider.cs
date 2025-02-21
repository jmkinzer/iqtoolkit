﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Data.OleDb;
using System.IO;

namespace IQToolkit.OleDb
{
    using Entities;
    using Entities.Data;

    /// <summary>
    /// An entity provider for OLEDB providers.
    /// </summary>
    public class OleDbEntityProvider : DbEntityProvider
    {
        protected OleDbEntityProvider(
            OleDbQueryExecutor executor,
            QueryLanguage language,
            EntityMapping? mapping,
            QueryPolicy? policy,
            TextWriter? log,
            QueryCache? cache,
            QueryOptions? options)
            : base(
                  executor,
                  language,
                  mapping,
                  policy,
                  log,
                  cache,
                  options)
        {
        }

        public OleDbEntityProvider(
            OleDbQueryExecutor executor,
            QueryLanguage language)
            : this(executor, language, null, null, null, null, null)
        {
        }

        public OleDbEntityProvider(
            OleDbConnection connection,
            QueryLanguage language)
            : this(new OleDbQueryExecutor(connection), language)
        {
        }

        public new OleDbQueryExecutor Executor => 
            (OleDbQueryExecutor)base.Executor;

        public new OleDbEntityProvider WithExecutor(QueryExecutor executor) =>
            (OleDbEntityProvider)With(executor: executor);

        public new OleDbEntityProvider WithLanguage(QueryLanguage language) =>
            (OleDbEntityProvider)With(language: language);

        public new OleDbEntityProvider WithMapping(EntityMapping mapping) =>
            (OleDbEntityProvider)With(mapping: mapping);

        public new OleDbEntityProvider WithPolicy(QueryPolicy policy) =>
            (OleDbEntityProvider)With(policy: policy);

        public new OleDbEntityProvider WithLog(TextWriter? log) =>
            (OleDbEntityProvider)With(log: log);

        public new OleDbEntityProvider WithCache(QueryCache? cache) =>
            (OleDbEntityProvider)With(cache: cache);

        public new OleDbEntityProvider WithOptions(QueryOptions options) =>
            (OleDbEntityProvider)With(options: options);

        protected override EntityProvider Construct(
            QueryExecutor executor,
            QueryLanguage language,
            EntityMapping? mapping,
            QueryPolicy? policy,
            TextWriter? log,
            QueryCache? cache,
            QueryOptions? options)
        {
            return new OleDbEntityProvider(
                (OleDbQueryExecutor)executor, 
                language, 
                mapping, 
                policy, 
                log, 
                cache,
                options
                );
        }
    }
}