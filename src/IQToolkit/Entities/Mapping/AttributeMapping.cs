// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;

namespace IQToolkit.Entities.Mapping
{
    using Utils;

    /// <summary>
    /// An <see cref="EntityMapping"/> that uses attributes on entity or context types
    /// to define the mapping.
    /// </summary>
    public class AttributeMapping : StandardMapping
    {
        private ImmutableDictionary<string, IReadOnlyList<MappingAttribute>> _idToAttributes;
        private ImmutableDictionary<MemberInfo, string> _contextMembertoEntityIdMap;
        private ImmutableDictionary<string, MemberInfo> _entityIdToContextMemberMap;

        /// <summary>
        /// Constructs a new instance of a <see cref="AttributeMapping"/> where mapping attributes are
        /// discovered on a context class (instead of from the entity types).
        /// </summary>
        /// <param name="contextType">The type of the context class that encodes the mapping attributes.
        /// If not spefied, the mapping attributes are assumed to be defined on the individual entity types.</param>
        public AttributeMapping(Type? contextType = null)
            : base(contextType)
        {
            _idToAttributes = ImmutableDictionary<string, IReadOnlyList<MappingAttribute>>.Empty;
            _contextMembertoEntityIdMap = ImmutableDictionary<MemberInfo, string>.Empty;
            _entityIdToContextMemberMap = ImmutableDictionary<string, MemberInfo>.Empty;

            if (contextType != null)
                this.InitializeContextMembers();
        }

        protected override string GetEntityId(Type entityType)
        {
            if (this.ContextType != null
                && this.TryGetContextMember(entityType, out var contextMember))
            {
                return GetEntityId(contextMember);
            }
            else
            {
                // look for entity id specified on type itself
                var attr = entityType.GetCustomAttribute<EntityAttribute>();
                if (attr != null && attr.Id != null)
                    return attr.Id;
            }

            // use the entity type name as the entity id
            return entityType.Name;
        }

        protected override string GetEntityId(MemberInfo contextMember)
        {
            if (!_contextMembertoEntityIdMap.TryGetValue(contextMember, out var id))
            {
                var entityAttr = contextMember.GetCustomAttribute<EntityAttribute>();

                var tmp = (entityAttr != null && !string.IsNullOrEmpty(entityAttr.Id))
                    ? entityAttr.Id
                    : id = base.GetEntityId(contextMember);

                id = ImmutableInterlocked.GetOrAdd(ref _contextMembertoEntityIdMap, contextMember, tmp);
                _entityIdToContextMemberMap = _entityIdToContextMemberMap.SetItem(id, contextMember);
            }

            return id;
        }

        protected override MappedEntity CreateEntity(
            Type entityType, string entityId)
        {
            return new StandardEntity(
                this,
                entityId,
                entityType,
                GetEntityRuntimeType(entityType, entityId),
                me => CreateEntityTables(me),
                me => CreateMembers(me, parent: null)
                );
        }

        protected virtual Type GetEntityRuntimeType(
            Type entityType, 
            string entityId)
        {
            var attr = this.GetOrCreateMappingAttributes(entityType, entityId)
                .OfType<EntityAttribute>()
                .FirstOrDefault();

            return attr != null && attr.ConstructedType != null
                ? attr.ConstructedType
                : entityType;
        }

        protected virtual IReadOnlyList<MappedTable> CreateEntityTables(
            MappedEntity entity)
        {
            var attrs = GetOrCreateMappingAttributes(entity.Type, entity.Id)
                .OfType<TableBaseAttribute>();

            var tableAttr = attrs.OfType<TableAttribute>().FirstOrDefault();
            var extTableAttrs = attrs.OfType<ExtensionTableAttribute>();

            var tables = new List<MappedTable>();
            tables.Add(CreateTable(entity, tableAttr?.Name ?? entity.Id));
            tables.AddRange(extTableAttrs.Select(ta => CreateTable(entity, ta.Name ?? entity.Id)));
            return tables.ToReadOnly();
        }

        protected virtual IReadOnlyList<MappedMember> CreateMembers(
            MappedEntity entity,
            MappedMember? parent)
        {
            var declaringType = parent != null
                ? TypeHelper.GetSequenceElementType(parent.Type)
                : entity.Type;

            var mappedMembers = new List<MappedMember>();

            var attrs = GetOrCreateMappingAttributes(entity.Type, entity.Id).OfType<MemberAttribute>().ToList();
            foreach (var attr in attrs)
            {
                if (attr.Member != null 
                    && entity.Type.TryGetDeclaredFieldOrPropertyFromPath(attr.Member, out var member)
                    && member.DeclaringType == declaringType)
                {
                    var mappedMember = CreateMember(entity, member, attr, parent);
                    mappedMembers.Add(mappedMember);
                }
            }

            return mappedMembers.ToReadOnly();
        }

        protected virtual MappedMember CreateMember(
            MappedEntity entity, 
            MemberInfo member, 
            MemberAttribute attr,
            MappedMember? parent)
        {
            switch (attr)
            {
                case ColumnAttribute columnAttr:
                    return new StandardColumnMember(
                        entity,
                        parent,
                        member,
                        me =>
                        {
                            var table = !string.IsNullOrEmpty(columnAttr.Table)
                                && entity.TryGetTable(columnAttr.Table, out var colTable)
                                ? colTable
                                : entity.PrimaryTable;
                            var colName = !string.IsNullOrEmpty(columnAttr.Name) 
                                ? columnAttr.Name 
                                : member.Name;
                            table.TryGetColumn(colName, out var column);
                            return column!;
                        });

                case CompoundAttribute compoundAttr:
                    return new StandardCompoundMember(
                        entity,
                        parent,
                        member,
                        compoundAttr.ConstructedType ?? TypeHelper.GetMemberType(member),
                        me => CreateMembers(me.Entity, me)
                        );
                case AssociationAttribute assocAttr:
                    return new StandardAssociationMember(
                        entity,
                        parent,
                        member,
                        assocAttr.IsForeignKey,
                        me => !string.IsNullOrEmpty(assocAttr.KeyColumns) 
                            ? this.GetEntityColumns(entity, assocAttr.KeyColumns)
                            : ReadOnlyList<MappedColumn>.Empty,
                        me =>
                        {
                            var relatedEntityType = TypeHelper.GetEntityType(member);
                            var relatedEntityId = !string.IsNullOrEmpty(assocAttr.RelatedEntityId) ? assocAttr.RelatedEntityId : this.GetEntityId(relatedEntityType);
                            return this.GetEntity(relatedEntityType, relatedEntityId);
                        },
                        me => 
                        {
                            if (!string.IsNullOrEmpty(assocAttr.RelatedKeyColumns))
                                return this.GetEntityColumns(me.RelatedEntity, assocAttr.RelatedKeyColumns);
                            if (!string.IsNullOrEmpty(assocAttr.KeyColumns))
                                return this.GetEntityColumns(me.RelatedEntity, assocAttr.KeyColumns);
                            return ReadOnlyList<MappedColumn>.Empty;
                        });

                default:
                    throw new InvalidOperationException($"AttributeMapping: The member '{entity.Type.Name}.{member.Name}' has an unknown mapping attribute '{attr.GetType().Name}'");
            }
        }

        protected virtual MappedTable CreateTable(
            MappedEntity entity, 
            string tableName)
        {
            var attr =
                this.GetOrCreateMappingAttributes(entity.Type, entity.Id)
                .OfType<TableBaseAttribute>()
                .FirstOrDefault(ta => (ta.Name ?? entity.Id) == tableName);

            var name = attr?.Name ?? entity.Id;

            if (attr is ExtensionTableAttribute exAttr)
            {
                return new StandardExtensionTable(
                    entity,
                    name,
                    me => CreateTableColumns(entity, me),
                    me => !string.IsNullOrEmpty(exAttr.KeyColumns)
                        ? GetTableColumns(entity.PrimaryTable, exAttr.KeyColumns)
                        : ReadOnlyList<MappedColumn>.Empty,
                    () => GetExtensionRelatedTable(entity, entity.PrimaryTable, exAttr),
                    me => !string.IsNullOrEmpty(exAttr.RelatedKeyColumns) ? GetTableColumns(me.RelatedTable, exAttr.RelatedKeyColumns) 
                           : !string.IsNullOrEmpty(exAttr.KeyColumns) ? GetTableColumns(me.RelatedTable, exAttr.KeyColumns)
                           : ReadOnlyList<MappedColumn>.Empty
                    );
            }
            else
            {
                return new StandardPrimaryTable(
                    entity,
                    name,
                    me => CreateTableColumns(entity, me)
                    );
            }
        }

        private static readonly char[] _nameListSeparators = new char[] { ' ', ',', '|' };

        private MappedTable GetExtensionRelatedTable(
            MappedEntity entity, MappedTable primaryTable, ExtensionTableAttribute attr)
        {
            if (!string.IsNullOrEmpty(attr.RelatedTableName)
                && entity.TryGetTable(attr.RelatedTableName, out var relatedTable))
            {
                return relatedTable;
            }
            else
            {
                // if related table is not specified, assume it is the primary table,
                // since extension tables typically extend the primary table.
                return primaryTable;
            }
        }

        private IReadOnlyList<string> GetExtensionKeyColumns(
            ExtensionTable table, ExtensionTableAttribute attr)
        {
            if (!string.IsNullOrEmpty(attr.KeyColumns))
            {
                return attr.KeyColumns.Split(_nameListSeparators).ToReadOnly();
            }
            else
            {
                return ReadOnlyList<string>.Empty;
            }
        }

        private IReadOnlyList<MappedColumn> GetExtensionRelatedColumns(
            ExtensionTable table, ExtensionTableAttribute attr)
        {
            var relatedKeyColumns = attr.RelatedKeyColumns ?? attr.KeyColumns;
            if (!string.IsNullOrEmpty(relatedKeyColumns))
            {
                var columns = new List<MappedColumn>();

                foreach (var keyName in relatedKeyColumns.Split(_nameListSeparators))
                {
                    if (table.RelatedTable.TryGetColumn(keyName, out var column))
                    {
                        columns.Add(column);
                    }
                }

                return columns.ToReadOnly();
            }
            else
            {
                return ReadOnlyList<MappedColumn>.Empty;
            }
        }

        private IReadOnlyList<MappedColumn> CreateTableColumns(
            MappedEntity entity, MappedTable table)
        {
            var attributes =
                this.GetOrCreateMappingAttributes(entity.Type, entity.Id);

            var columns = new List<MappedColumn>();
            var columnNameToColumnMap = new Dictionary<string, MappedColumn>();

            var columnAttrs = attributes.OfType<ColumnAttribute>().ToList();

            // find all mapped member columns first
            foreach (var columnAttr in columnAttrs)
            {
                var memberName = columnAttr.Member;
                if (memberName != null
                    && entity.Type.TryGetDeclaredFieldOrPropertyFromPath(memberName, out var member))
                {
                    var columnName = !string.IsNullOrEmpty(columnAttr.Name)
                        ? columnAttr.Name
                        : member.Name;

                    var columnTable = (!string.IsNullOrEmpty(columnAttr.Table)
                            && entity.TryGetTable(columnAttr.Table, out var ctable))
                        ? ctable
                        : entity.PrimaryTable;

                    var columnType = !string.IsNullOrEmpty(columnAttr.DbType)
                        ? columnAttr.DbType
                        : null;

                    if (!columnNameToColumnMap.TryGetValue(columnName, out var column))
                    {
                        column = new StandardColumn(
                            columnTable,
                            columnName,
                            columnType,
                            isPrimaryKey: columnAttr.IsPrimaryKey,
                            isReadOnly: columnAttr.IsReadOnly,
                            isComputed: columnAttr.IsComputed,
                            isGenerated: columnAttr.IsGenerated,
                            me => entity.Members.OfType<ColumnMember>().FirstOrDefault(cm => cm.Column == me)
                            )
                        {
                        };
                        columns.Add(column);
                        columnNameToColumnMap[column.Name] = column;
                    }
                }
            }

            // find columns listed in extension tables that refer to this table
            foreach (var tableAttr in attributes.OfType<ExtensionTableAttribute>())
            {
                if (tableAttr.RelatedTableName == table.Name
                    || (string.IsNullOrEmpty(tableAttr.RelatedTableName) && table == entity.PrimaryTable))
                {
                    var names = !string.IsNullOrEmpty(tableAttr.RelatedKeyColumns) ? tableAttr.RelatedKeyColumns
                        : !string.IsNullOrEmpty(tableAttr.KeyColumns) ? tableAttr.KeyColumns
                        : "";

                    foreach (var keyColumnName in GetNames(names))
                    {
                        if (!columnNameToColumnMap.ContainsKey(keyColumnName))
                        {
                            // create column, but we don't know anything about it
                            var column = new StandardColumn(
                                table,
                                keyColumnName,
                                columnType: null,
                                isPrimaryKey: false,
                                isReadOnly: false,
                                isComputed: false,
                                isGenerated: false,
                                fnMember: null
                                );
                            columns.Add(column);
                            columnNameToColumnMap[column.Name] = column;
                        }
                    }
                }
            }

            return columns.ToList();
        }

        #region Mapping Attributes

        protected virtual IReadOnlyList<MappingAttribute> GetOrCreateMappingAttributes(
            Type entityType, string entityId)
        {
            if (!_idToAttributes.TryGetValue(entityId, out var attrs))
            {
                var tmp = CreateMappingAttributes(entityType, entityId);
                attrs = ImmutableInterlocked.GetOrAdd(ref _idToAttributes, entityId, tmp);
            }

            return attrs;
        }

        /// <summary>
        /// Creates the list of <see cref="MappingAttribute"/> for the entity.
        /// </summary>
        protected virtual IReadOnlyList<MappingAttribute> CreateMappingAttributes(
            Type entityType, string entityId)
        {
            var attributes = new List<MappingAttribute>();

            _entityIdToContextMemberMap.TryGetValue(entityId, out var contextMember);

            this.GetDeclaredMappingAttributes(entityType, entityId, attributes);

            // if no entity attribute is mentioned, add one
            if (!attributes.OfType<EntityAttribute>().Any())
            {
                attributes.Add(new EntityAttribute { ConstructedType = entityType });
            }

            // if no table attribute is mentioned, add one based on the context member or entity type
            var tableAttr = attributes.OfType<TableAttribute>().FirstOrDefault();
            if (tableAttr == null)
            {
                attributes.Add(new TableAttribute { Name = contextMember?.Name ?? entityType.Name });
            }
            else if (string.IsNullOrEmpty(tableAttr.Name))
            {
                tableAttr.Name = contextMember?.Name ?? entityType.Name;
            }

            // add any implicit member mappings
            var memberToAttributeMap = new Dictionary<MemberInfo, MemberAttribute>();
            foreach (var attr in attributes)
            {
                if (attr is MemberAttribute ma 
                    && ma.Member != null
                    && entityType.TryGetDeclaredFieldOrPropertyFromPath(ma.Member, out var member))
                {
                    memberToAttributeMap[member] = ma;
                }
            }

            AddImplicitlyMappedMembers(entityType, memberToAttributeMap, attributes, "");

            return attributes.ToReadOnly();
        }

        /// <summary>
        /// Adds attributes for members that were not explicitly mapped, 
        /// but should be implicitly mapped.
        /// </summary>
        private void AddImplicitlyMappedMembers(
            Type type, 
            Dictionary<MemberInfo, MemberAttribute> memberToAttributeMap, 
            List<MappingAttribute> list,
            string path)
        {
            // look for members that are not explicitly mapped and create column mappings for them.
            var dataMembers = type.GetDeclaredFieldsAndProperties();

            foreach (var member in dataMembers)
            {
                var memberPath = CombinePath(path, member.Name);

                var memberType = TypeHelper.GetMemberType(member);
                if (IsPossibleColumnMember(member))
                {
                    // members with scalar type are assumed to be columns
                    if (!memberToAttributeMap.ContainsKey(member))
                    {
                        var attr = new ColumnAttribute { Member = memberPath };
                        list.Add(attr);
                        memberToAttributeMap.Add(member, attr);
                    }
                }
                else if (IsPossibleCompoundMember(member))
                {
                    // members with non-sequence/non-scalar types are assumed to be nested entities
                    if (!memberToAttributeMap.TryGetValue(member, out var attr))
                    {
                        attr = new CompoundAttribute { Member = memberPath };
                        list.Add(attr);
                        memberToAttributeMap.Add(member, attr);
                    }

                    if (attr is CompoundAttribute)
                    {
                        // look for unmapped members of the compound member
                        AddImplicitlyMappedMembers(memberType, memberToAttributeMap, list, memberPath);
                    }
                }
            }
        }

        private string CombinePath(string basePath, string member)
        {
            if (basePath == "")
                return member;
            return basePath + "." + member;
        }

        /// <summary>
        /// Gets the <see cref="MappingAttribute"/> declared by the user for the entity type.
        /// </summary>
        protected virtual void GetDeclaredMappingAttributes(
            Type entityType, string entityId, List<MappingAttribute> list)
        {
            if (this.ContextType != null
                && this.TryGetContextMember(entityId, out var contextMember))
            { 
                this.GetMemberMappingAttributes(contextMember, list);
            }

            this.GetTypeMappingAttributes(entityType, list);
        }

        private void GetMemberMappingAttributes(MemberInfo member, List<MappingAttribute> list)
        {
            foreach (var attr in member.GetCustomAttributes<MappingAttribute>())
            {
                if (attr is MemberAttribute ma && ma.Member == null)
                {
                    ma.Member = member.Name;
                }

                list.Add(attr);
            }
        }

        private void GetTypeMappingAttributes(Type entityType, List<MappingAttribute> list)
        {
            // get attributes from entity type itself
            foreach (var ma in entityType.GetCustomAttributes<MappingAttribute>())
            {
                var entity = ma as EntityAttribute;
                if (entity != null && entity.ConstructedType == null)
                {
                    entity.ConstructedType = entityType;
                }

                var table = ma as TableAttribute;
                if (table != null && string.IsNullOrEmpty(table.Name))
                {
                    table.Name = entityType.Name;
                }

                list.Add(ma);
            }

            foreach (var member in TypeHelper.GetDeclaredFieldsAndProperties(entityType, includeNonPublic: true))
            {
                this.GetMemberMappingAttributes(member, list);
            }
        }

#endregion
    }
}
