// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using IQToolkit.Utils;
using System.Collections.Generic;
using System.Linq;

namespace IQToolkit.Entities.Mapping
{
    public static class MappedEntityExtensions
    {
        public static IReadOnlyList<ColumnMember> GetColumnMembers(
            this MappedEntity entity,
            IEnumerable<MappedColumn> columns)
        {
            var list = new List<ColumnMember>();

            foreach (var column in columns)
            {
                if (column.Member != null)
                {
                    list.Add(column.Member);
                }
            }

            return list.ToReadOnly();
        }
    }
}