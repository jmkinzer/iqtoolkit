// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;

namespace IQToolkit.Entities.Mapping
{
    /// <summary>
    /// A <see cref="MappingAttribute"/> that describes an association between two entities via related column
    /// values in the tables underlying each. This is often the same as a foreign key relationship in the database.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
    public class AssociationAttribute : MemberAttribute
    {
        /// <summary>
        /// One or more columns used to associate this entity with the other related entities.
        /// </summary>
        public string? KeyColumns { get; set; }

        /// <summary>
        /// The mapping ID of the related entity.
        /// </summary>
        public string? RelatedEntityId { get; set; }

        /// <summary>
        /// One or more columns from the tables associated with the related entity.
        /// </summary>
        public string? RelatedKeyColumns { get; set; }

        /// <summary>
        /// True if the <see cref="KeyColumns"/> are a foreign key referring to the related entity's table primary key.
        /// </summary>
        public bool IsForeignKey { get; set; }
    }
}
