﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

namespace IQToolkit.Entities.Mapping
{
    /// <summary>
    /// Defined on types that can describe an <see cref="MappedEntity"/>.
    /// </summary>
    public interface IHaveMappingEntity
    {
        /// <summary>
        /// The <see cref="MappedEntity"/>.
        /// </summary>
        MappedEntity Entity { get; }
    }
}