// Copyright(c) Microsoft Corporation.All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace IQToolkit.Entities.Mapping
{
    public abstract class MappedTable
    {
        public abstract MappedEntity Entity { get; }

        /// <summary>
        /// The name of the table.
        /// </summary>
        public abstract string Name { get; }

        /// <summary>
        /// All the columns of this table.
        /// </summary>
        public abstract IReadOnlyList<MappedColumn> Columns { get; }

        /// <summary>
        /// Gets the column for the column name.
        /// </summary>
        public abstract bool TryGetColumn(string name, [NotNullWhen(true)] out MappedColumn? column);
    }

    public abstract class PrimaryTable : MappedTable
    {
    }

    public abstract class ExtensionTable : MappedTable
    {
        /// <summary>
        /// Gets the columns in the extension table that correspond to the related table's column.
        /// </summary>
        public abstract IReadOnlyList<MappedColumn> KeyColumns { get; }

        /// <summary>
        /// The table that this table is an extension of.
        /// </summary>
        public abstract MappedTable RelatedTable { get; }

        /// <summary>
        /// Gets the column names in the related table that correspond to the columns from the extension table.
        /// </summary>
        public abstract IReadOnlyList<MappedColumn> RelatedKeyColumns { get; }
    }

    public abstract class MappedColumn
    {
        /// <summary>
        /// The table the column is within.
        /// </summary>
        public abstract MappedTable Table { get; }

        /// <summary>
        /// The entity member that refers to this column.
        /// </summary>
        public abstract ColumnMember? Member { get; }

        /// <summary>
        /// The name of the column
        /// </summary>
        public abstract string Name { get; }

        /// <summary>
        /// The column's type in the database query language.
        /// </summary>
        public abstract string? Type { get; }

        /// <summary>
        /// True if the column is part of the table's primary key
        /// </summary>
        public abstract bool IsPrimaryKey { get; }

        /// <summary>
        /// True if is the column should not be updated.
        /// </summary>
        public abstract bool IsReadOnly { get; }

        /// <summary>
        /// True if the column is computed after insert or update of the corresponding row.
        /// </summary>
        public abstract bool IsComputed { get; }

        /// <summary>
        /// True the column is generated on the server during insertion.
        /// </summary>
        public abstract bool IsGenerated { get; }

        /// <summary>
        /// True the column is updatable
        /// </summary>
        public virtual bool IsUpdatable =>
            !this.IsPrimaryKey
            && !this.IsReadOnly;
    }
}