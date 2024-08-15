// Copyright(c) Microsoft Corporation.All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace IQToolkit.Entities.Mapping
{
    using Utils;

    /// <summary>
    /// A base class for a member of an entity type that is mapped to one or more database columns.
    /// </summary>
    public abstract class MappedMember
    {
        /// <summary>
        /// The <see cref="MappedEntity"/> this is a member of.
        /// </summary>
        public abstract MappedEntity Entity { get; }

        /// <summary>
        /// The parent of this member if it is nested within another member.
        /// </summary>
        public abstract MappedMember? Parent { get; }

        /// <summary>
        /// The member that is mapped.
        /// </summary>
        public abstract MemberInfo Member { get; }

        /// <summary>
        /// The type of the member.
        /// </summary>
        public virtual Type Type => TypeHelper.GetMemberType(this.Member);
    }

    /// <summary>
    /// A member that is mapped to a single column.
    /// </summary>
    public abstract class ColumnMember : MappedMember
    {
        /// <summary>
        /// The column that is mapped.
        /// </summary>
        public abstract MappedColumn Column { get; }
    }

    /// <summary>
    /// A member that is constructed from and composed of multiple mapped members.
    /// </summary>
    public abstract class CompoundMember : MappedMember
    {
        /// <summary>
        /// The constructed type of the compound member, if it differs from the member type.
        /// </summary>
        public abstract Type ConstructedType { get; }

        /// <summary>
        /// The mapped members for the type.
        /// </summary>
        public abstract IReadOnlyList<MappedMember> Members { get; }
    }

    /// <summary>
    /// The base class for any member that refers to another entity or a collection of entities.
    /// </summary>
    public abstract class RelationshipMember : MappedMember
    {
        /// <summary>
        /// The entity on the other side of the relationship.
        /// </summary>
        public abstract MappedEntity RelatedEntity { get; }

        /// <summary>
        /// Returns true if the member is the source of a one-to-many relationship.
        /// </summary>
        public abstract bool IsSource { get; }

        /// <summary>
        /// Returns true if the member is the target of a one-to-many relationship.
        /// </summary>
        public abstract bool IsTarget { get; }

        /// <summary>
        /// Determines if a relationship property is a one-to-one relationship.
        /// </summary>
        public abstract bool IsOneToOne { get; }
    }

    /// <summary>
    /// A <see cref="RelationshipMember"/> that is a join of two database tables.
    /// </summary>
    public abstract class AssociationMember : RelationshipMember
    {
        /// <summary>
        /// Returns the columns on this side of the association
        /// </summary>
        public abstract IReadOnlyList<MappedColumn> KeyColumns { get; }

        /// <summary>
        /// Returns the key columns on the other side (related side) of the association
        /// </summary>
        public abstract IReadOnlyList<MappedColumn> RelatedKeyColumns { get; }
    }
}