﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;

namespace IQToolkit.Entities.Factories
{
    public abstract class EntityProviderFactory
    {
        /// <summary>
        /// The name of the factory.
        /// </summary>
        public abstract string Name { get; }

        /// <summary>
        /// Creates an <see cref="EntityProvider"/> for the connection string, 
        /// if the connection string is compatible.
        /// </summary>
        public virtual bool TryCreateProviderForConnection(string connectionString, out IEntityProvider provider)
        {
            provider = default!;
            return false;
        }

        /// <summary>
        /// Creates an <see cref="EntityProvider"/> for the file path, 
        /// if the file path is compatible.
        /// </summary>
        public virtual bool TryCreateProviderForFilePath(string filePath, out IEntityProvider provider)
        {
            provider = default!;
            return false;
        }

        /// <summary>
        /// Creates an <see cref="EntityProvider"/> for the connection string, 
        /// if the connection string is compatible.
        /// </summary>
        public IEntityProvider CreateProviderForConnection(string connectionString)
        {
            if (TryCreateProviderForConnection(connectionString, out var provider))
                return provider;
            throw new InvalidOperationException($"Cannot create provider for the connection string.");
        }

        public IEntityProvider CreateProviderForFilePath(string filePath)
        {
            if (TryCreateProviderForFilePath(filePath, out var provider))
                return provider;
            throw new InvalidOperationException($"Cannot create provider for the file path.");
        }
    }
}