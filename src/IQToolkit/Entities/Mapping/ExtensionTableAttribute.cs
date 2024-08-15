﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;

namespace IQToolkit.Entities.Mapping
{
    /// <summary>
    /// Describes the mapping between additional database tables and an entity type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Interface | AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
    public class ExtensionTableAttribute : TableBaseAttribute
    {
        /// <summary>
        /// The columns in the extension table that correspond to columns in the primary table.
        /// Must be specified.
        /// </summary>
        public string? KeyColumns { get; set; }

        /// <summary>
        /// The name of the related table used in advanced multi-table mapping.
        /// </summary>
        public string? RelatedTableName { get; set; }

        /// <summary>
        /// The columns in the related table that correspond to the key columns in the extension table.
        /// If not specified, it is assumed the column names from both tables are the same.
        /// </summary>
        public string? RelatedKeyColumns { get; set; }
    }
}
