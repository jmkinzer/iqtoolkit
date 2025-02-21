﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

namespace IQToolkit.Entities.Mapping
{
    /// <summary>
    /// A pairing between an entity instance and its mapping.
    /// </summary>
    public struct EntityInfo
    {
        /// <summary>
        /// The entity instance.
        /// </summary>
        public object Instance { get; }

        /// <summary>
        /// The mapping used for the entity.
        /// </summary>
        public MappedEntity Mapping { get; }

        /// <summary>
        /// Construct a new <see cref="EntityInfo"/>.
        /// </summary>
        public EntityInfo(object instance, MappedEntity mapping)
        {
            this.Instance = instance;
            this.Mapping = mapping;
        }
    }
}