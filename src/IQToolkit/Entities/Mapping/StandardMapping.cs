// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace IQToolkit.Entities.Mapping
{
    using System.Collections.Immutable;
    using System.Diagnostics.CodeAnalysis;
    using Utils;

    public abstract class StandardMapping : EntityMapping
    {
        private ImmutableDictionary<string, MappedEntity> _idToEntityMap;
        private ImmutableDictionary<string, MappedTable> _nameToTableMap;

        public override Type? ContextType { get; }

        /// <summary>
        /// The set of members that refer to entity tables on the context type.
        /// </summary>
        public override IReadOnlyList<MemberInfo> ContextMembers =>
            _contextMembers.Value;
        private readonly Lazy<IReadOnlyList<MemberInfo>> _contextMembers;

        public StandardMapping(Type? contextType)
        {
            this.ContextType = contextType;
            _idToEntityMap = ImmutableDictionary<string, MappedEntity>.Empty;
            _nameToTableMap = ImmutableDictionary<string, MappedTable>.Empty;

            _contextMembers = new Lazy<IReadOnlyList<MemberInfo>>(() =>
                contextType != null
                    ? TypeHelper.GetDeclaredFieldsAndProperties(
                        contextType,
                        m => TypeHelper.IsAssignableToGeneric(TypeHelper.GetMemberType(m), typeof(IQueryable<>))
                        )
                    : ReadOnlyList<MemberInfo>.Empty
            );
        }

        protected virtual void InitializeContextMembers()
        {
            // pre-load entities for context members
            foreach (var m in this.ContextMembers)
            {
                InitializeEntity(GetEntity(m));
            }
        }

        protected virtual void InitializeEntity(MappedEntity entity)
        {
            foreach (var table in entity.Tables)
            {
                InitializeTable(table);
            }

            foreach (var member in entity.Members)
            {
                InitializeMember(member);
            }
        }

        protected virtual void InitializeTable(MappedTable table)
        {
            var columns = table.Columns;
            if (columns.Count > 0)
            {
                table.TryGetColumn(columns[0].Name, out _);
            }
            if (table is ExtensionTable extTable)
            {
                var keyColumns = extTable.KeyColumns;
                var relatedTable = extTable.RelatedTable;
                var relatedKeyColumns = extTable.RelatedKeyColumns;
            }
        }

        protected virtual void InitializeMember(MappedMember member)
        {
            if (member is ColumnMember mcm)
            {
                var column = mcm.Column;
            }
            else if (member is CompoundMember compound)
            {
                foreach (var cm in compound.Members)
                {
                    InitializeMember(cm);
                }
            }
            else if (member is AssociationMember assoc)
            {
                var keyColumns = assoc.KeyColumns;
                var relatedEntity = assoc.RelatedEntity;
                var relatedColumns = assoc.RelatedKeyColumns;
            }
        }

        public override IReadOnlyList<MappedEntity> GetEntities()
        {
            return _idToEntityMap.Values.ToReadOnly();
        }

        /// <summary>
        /// Gets the entity id for the context member.
        /// </summary>
        protected virtual string GetEntityId(
            MemberInfo contextMember)
        {
            return contextMember.Name;
        }

        /// <summary>
        /// Gets the entity id for the entity type.
        /// </summary>
        protected virtual string GetEntityId(
            Type entityType)
        {
            if (TryGetContextMember(entityType, out var member))
            {
                return GetEntityId(member);
            }

            return entityType.Name;
        }

        /// <summary>
        /// Get the <see cref="MappedEntity"/> represented by the IQueryable context member
        /// </summary>
        public override MappedEntity GetEntity(
            MemberInfo contextMember)
        {
            var entityType = TypeHelper.GetEntityType(contextMember);
            return GetEntity(entityType, GetEntityId(contextMember));
        }

        /// <summary>
        /// Gets the context member associated with the entity id.
        /// </summary>
        public virtual bool TryGetContextMember(
            string entityId, 
            [NotNullWhen(true)] out MemberInfo member)
        {
            member = this.ContextMembers.FirstOrDefault(m => GetEntityId(m) == entityId);
            return member != null;
        }

        /// <summary>
        /// Gets the context member associated with the entity type.
        /// </summary>
        public virtual bool TryGetContextMember(
            Type entityType, 
            [NotNullWhen(true)] out MemberInfo member)
        {
            member = this.ContextMembers
                .FirstOrDefault(m => TypeHelper.GetEntityType(m) == entityType);
            return member != null;
        }

        /// <summary>
        /// Gets the <see cref="MappedEntity"/> for the entity id.
        /// </summary>
        public override MappedEntity GetEntity(
            Type entityType, string? entityId)
        {
            return GetOrCreateEntity(
                entityType,
                entityId ?? GetEntityId(entityType)
                );
        }

        /// <summary>
        /// Gets or creates the <see cref="MappedEntity"/> for the entity id.
        /// </summary>
        protected virtual MappedEntity GetOrCreateEntity(
            Type entityType, 
            string entityId)
        {
            if (!_idToEntityMap.TryGetValue(entityId, out var mappedEntity))
            {
                var tmp = CreateEntity(entityType, entityId)!;
                mappedEntity = ImmutableInterlocked.GetOrAdd(ref _idToEntityMap, entityId, tmp);
            }

            return mappedEntity;
        }

        /// <summary>
        /// Create a <see cref="MappedEntity"/> from attributes on the entity or context type.
        /// </summary>
        protected abstract MappedEntity CreateEntity(
            Type entityType, string entityId);

        private static readonly char[] _nameListSeparators = new char[] { ' ', ',', '|' };

        protected virtual IEnumerable<string> GetNames(string nameList) =>
            nameList.Split(_nameListSeparators);

        protected virtual IReadOnlyList<MappedColumn> GetEntityColumns(
            MappedEntity entity, 
            string columnNames,
            string? tableName = null)
        {
            var columns = new List<MappedColumn>();

            foreach (var columnName in GetNames(columnNames))
            {
                if (entity.TryGetColumn(columnName, tableName, out var column))
                {
                    columns.Add(column);
                }
            }

            return columns.ToReadOnly();
        }

        protected virtual IReadOnlyList<MappedColumn> GetTableColumns(
            MappedTable table, 
            string columnNames)
        {
            var columns = new List<MappedColumn>();

            foreach (var columnName in GetNames(columnNames))
            {
                if (table.TryGetColumn(columnName, out var column))
                {
                    columns.Add(column);
                }
            }

            return columns.ToReadOnly();
        }

        /// <summary>
        /// True if the member can possibly be a column member.
        /// </summary>
        protected virtual bool IsPossibleColumnMember(MemberInfo member)
        {
            if (member is Type)
                return false;

            var type = TypeHelper.GetNonNullableType(TypeHelper.GetMemberType(member));

            if (TypeHelper.IsSequenceType(type, out var elementType))
            {
                return elementType == typeof(char) || elementType == typeof(byte);
            }

            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Empty:
                    return false;
                case TypeCode.Object:
                    return
                        type == typeof(DateTimeOffset) ||
                        type == typeof(TimeSpan) ||
                        type == typeof(Guid) ||
                        typeof(IEnumerable<char>).IsAssignableFrom(type) ||
                        typeof(IEnumerable<byte>).IsAssignableFrom(type);
                default:
                    return true;
            }
        }

        /// <summary>
        /// True if the member can possibly be a compound (multi-column) member.
        /// </summary>
        protected virtual bool IsPossibleCompoundMember(MemberInfo member)
        {
            // TODO: discount any known entity types
            return !TypeHelper.IsSequenceType(TypeHelper.GetMemberType(member))
                && !IsPossibleColumnMember(member)
                && !(member is Type)
                && !IsKnownEntityType(TypeHelper.GetEntityType(member));
        }

        protected virtual bool IsKnownEntityType(Type type)
        {
            return this.ContextMembers.Any(m => TypeHelper.GetEntityType(m) == type);
        }

        protected class StandardEntity : MappedEntity
        {
            public StandardMapping Mapping { get; }
            public override string Id { get; }
            public override Type Type { get; }
            public override Type ConstructedType { get; }

            public override IReadOnlyList<MappedMember> Members => _mappedMembers.Value;
            private readonly Lazy<IReadOnlyList<MappedMember>> _mappedMembers;

            public override IReadOnlyList<MappedTable> Tables => _mappedTables.Value;
            private readonly Lazy<IReadOnlyList<MappedTable>> _mappedTables;

            public override IReadOnlyList<ColumnMember> PrimaryKeyMembers => _primaryKeyMembers.Value;
            private readonly Lazy<IReadOnlyList<ColumnMember>> _primaryKeyMembers;

            public override MappedTable PrimaryTable => _primaryTable.Value;
            private readonly Lazy<MappedTable> _primaryTable;

            public override IReadOnlyList<ExtensionTable> ExtensionTables => _extensionTables.Value;
            private readonly Lazy<IReadOnlyList<ExtensionTable>> _extensionTables;

            public override IReadOnlyList<MappedColumn> Columns => _columns.Value;
            private readonly Lazy<IReadOnlyList<MappedColumn>> _columns;

            public override bool TryGetMember(string name, [NotNullWhen(true)] out MappedMember? member) =>
                _memberMap.Value.TryGetValue(name, out member);
            private readonly Lazy<Dictionary<string, MappedMember>> _memberMap;

            public override bool TryGetTable(string name, [NotNullWhen(true)] out MappedTable? table) =>
                _tableMap.Value.TryGetValue(name, out table);
            private readonly Lazy<Dictionary<string, MappedTable>> _tableMap;

            public StandardEntity(
                StandardMapping mapping, 
                string entityId,
                Type type,
                Type constructedType,
                Func<MappedEntity, IReadOnlyList<MappedTable>> fnTables,
                Func<MappedEntity, IReadOnlyList<MappedMember>> fnMembers)
            {
                this.Mapping = mapping;
                this.Id = entityId;
                this.Type = type;
                this.ConstructedType = constructedType;

                _mappedMembers = new Lazy<IReadOnlyList<MappedMember>>(
                    () => fnMembers(this), ReadOnlyList<MappedMember>.Empty
                    );

                _primaryKeyMembers = new Lazy<IReadOnlyList<ColumnMember>>(
                    () => this.Members.OfType<ColumnMember>().Where(m => m.Column.IsPrimaryKey).ToReadOnly(),
                    ReadOnlyList<ColumnMember>.Empty
                    );

                _mappedTables = new Lazy<IReadOnlyList<MappedTable>>(
                    () => fnTables(this), ReadOnlyList<MappedTable>.Empty
                    );

                _primaryTable = new Lazy<MappedTable>(() =>
                    this.Tables.First(t => !(t is ExtensionTable))
                    );

                _extensionTables = new Lazy<IReadOnlyList<ExtensionTable>>(
                    () => this.Tables.OfType<ExtensionTable>().ToReadOnly(),
                    ReadOnlyList<ExtensionTable>.Empty
                    );

                _columns = new Lazy<IReadOnlyList<MappedColumn>>(
                    () => this.Tables.SelectMany(t => t.Columns).ToReadOnly()
                    );

                _tableMap = new Lazy<Dictionary<string, MappedTable>>(
                    () => _mappedTables.Value.ToDictionary(t => t.Name)
                    );

                _memberMap = new Lazy<Dictionary<string, MappedMember>>(
                    () => _mappedMembers.Value.ToDictionary(m => m.Member.Name)
                    );
            }
        }

        protected class StandardPrimaryTable : PrimaryTable
        {
            public override MappedEntity Entity { get; }
            public override string Name { get; }

            public override IReadOnlyList<MappedColumn> Columns => _columns.Value;
            private readonly Lazy<IReadOnlyList<MappedColumn>> _columns;

            public override bool TryGetColumn(string name, [NotNullWhen(true)] out MappedColumn? column) =>
                _nameToColumnMap.Value.TryGetValue(name, out column);
            private readonly Lazy<Dictionary<string, MappedColumn>> _nameToColumnMap;

            public StandardPrimaryTable(
                MappedEntity entity,
                string tableName,
                Func<MappedTable, IReadOnlyList<MappedColumn>> fnColumns)
            {
                this.Entity = entity;
                this.Name = tableName;

                _columns = new Lazy<IReadOnlyList<MappedColumn>>(
                    () => fnColumns(this)
                    );

                _nameToColumnMap = new Lazy<Dictionary<string, MappedColumn>>(() =>
                    _columns.Value.ToDictionary(c => c.Name)
                    );
            }
        }

        protected class StandardExtensionTable : ExtensionTable
        {
            public override MappedEntity Entity { get; }
            public override string Name { get; }

            private readonly Lazy<IReadOnlyList<MappedColumn>> _columns;
            public override IReadOnlyList<MappedColumn> Columns =>
                _columns.Value;

            public override bool TryGetColumn(string name, [NotNullWhen(true)] out MappedColumn? column) =>
                _nameToColumnMap.Value.TryGetValue(name, out column);
            private readonly Lazy<Dictionary<string, MappedColumn>> _nameToColumnMap;

            private readonly Lazy<IReadOnlyList<MappedColumn>> _keyColumns;
            public override IReadOnlyList<MappedColumn> KeyColumns =>
                _keyColumns.Value;

            private readonly Lazy<MappedTable> _relatedTable;
            public override MappedTable RelatedTable =>
                _relatedTable.Value;

            private readonly Lazy<IReadOnlyList<MappedColumn>> _relatedKeyColumns;
            public override IReadOnlyList<MappedColumn> RelatedKeyColumns =>
                _relatedKeyColumns.Value;

            public StandardExtensionTable(
                MappedEntity entity,
                string tableName,
                Func<MappedTable, IReadOnlyList<MappedColumn>> fnColumns,
                Func<ExtensionTable, IReadOnlyList<MappedColumn>> fnKeyColumns,
                Func<MappedTable> fnRelatedTable,
                Func<ExtensionTable, IReadOnlyList<MappedColumn>> fnRelatedKeyColumns)
            {
                this.Entity = entity;
                this.Name = tableName;

                _columns = new Lazy<IReadOnlyList<MappedColumn>>(
                    () => fnColumns(this)
                    );

                _nameToColumnMap = new Lazy<Dictionary<string, MappedColumn>>(() =>
                    _columns.Value.ToDictionary(c => c.Name)
                    );

                _keyColumns = new Lazy<IReadOnlyList<MappedColumn>>(
                    () => fnKeyColumns(this),
                    ReadOnlyList<MappedColumn>.Empty
                    );

                _relatedTable = new Lazy<MappedTable>(
                    fnRelatedTable
                    );

                _relatedKeyColumns = new Lazy<IReadOnlyList<MappedColumn>>(
                    () => fnRelatedKeyColumns(this),
                    ReadOnlyList<MappedColumn>.Empty
                    );
            }
        }

        protected class StandardColumn : MappedColumn
        {
            public override MappedTable Table { get; }
            public override string Name { get; }
            public override string? Type { get; }
            public override bool IsPrimaryKey { get; }
            public override bool IsReadOnly { get; }
            public override bool IsComputed { get; }
            public override bool IsGenerated { get; }

            public override ColumnMember? Member => _member?.Value;
            private readonly Lazy<ColumnMember?>? _member;

            public StandardColumn(
                MappedTable table,
                string name,
                string? columnType,
                bool isPrimaryKey,
                bool isReadOnly,
                bool isComputed,
                bool isGenerated,
                Func<MappedColumn, ColumnMember?>? fnMember)
            {
                this.Table = table;
                this.Name = name;
                this.Type = columnType;
                this.IsPrimaryKey = isPrimaryKey;
                this.IsReadOnly = isReadOnly;
                this.IsComputed = isComputed;
                this.IsGenerated = isGenerated;
                _member = fnMember != null
                    ? new Lazy<ColumnMember?>(() => fnMember(this))
                    : null;
            }
        }

        protected class StandardColumnMember : ColumnMember
        {
            /// <summary>
            /// The entity this column member is part of.
            /// </summary>
            public override MappedEntity Entity { get; }

            /// <summary>
            /// The parent member if this member is nested within another member.
            /// </summary>
            public override MappedMember? Parent { get; }

            /// <summary>
            /// The member of the entity type.
            /// </summary>
            public override MemberInfo Member { get; }

            /// <summary>
            /// The table column that the member is mapped to.
            /// </summary>
            public override MappedColumn Column => _column.Value;
            private readonly Lazy<MappedColumn> _column;

            public StandardColumnMember(
                MappedEntity entity,
                MappedMember? parent,
                MemberInfo member,
                Func<StandardColumnMember, MappedColumn> fnColumn
                )
            {
                this.Entity = entity;
                this.Parent = parent;
                this.Member = member;
                _column = new Lazy<MappedColumn>(() => fnColumn(this));
            }
        }

        protected class StandardCompoundMember : CompoundMember
        {
            public override MappedEntity Entity { get; }
            public override MappedMember? Parent { get; }
            public override MemberInfo Member { get; }
            public override Type ConstructedType { get; }

            public override IReadOnlyList<MappedMember> Members => _members.Value;
            private readonly Lazy<IReadOnlyList<MappedMember>> _members;

            public StandardCompoundMember(
                MappedEntity entity,
                MappedMember? parent,
                MemberInfo member,
                Type constructedType,
                Func<MappedMember, IReadOnlyList<MappedMember>> fnMembers)
            {
                this.Entity = entity;
                this.Parent = parent;
                this.Member = member;
                this.ConstructedType = constructedType;
                _members = new Lazy<IReadOnlyList<MappedMember>>(() => fnMembers(this));
            }
        }

        protected class StandardAssociationMember : AssociationMember
        {
            public override MappedEntity Entity { get; }
            public override MappedMember? Parent { get; }
            public override MemberInfo Member { get; }
            public override bool IsSource { get; }

            public override bool IsTarget =>
                !IsSource;

            public override bool IsOneToOne =>
                !TypeHelper.IsSequenceType(TypeHelper.GetMemberType(this.Member));

            private readonly Lazy<IReadOnlyList<MappedColumn>> _keyMembers;
            public override IReadOnlyList<MappedColumn> KeyColumns =>
                _keyMembers.Value;

            private readonly Lazy<MappedEntity> _relatedEntity;
            public override MappedEntity RelatedEntity =>
                _relatedEntity.Value;

            private readonly Lazy<IReadOnlyList<MappedColumn>> _relatedKeyMembers;
            public override IReadOnlyList<MappedColumn> RelatedKeyColumns =>
                _relatedKeyMembers.Value;

            public StandardAssociationMember(
                MappedEntity entity,
                MappedMember? parent,
                MemberInfo member,
                bool isSource,
                Func<AssociationMember, IReadOnlyList<MappedColumn>> fnKeyColumns,
                Func<AssociationMember, MappedEntity> fnRelatedEntity,
                Func<AssociationMember, IReadOnlyList<MappedColumn>> fnRelatedKeyColumns)
            {
                this.Entity = entity;
                this.Parent = parent;
                this.Member = member;
                this.IsSource = isSource;
                _keyMembers = new Lazy<IReadOnlyList<MappedColumn>>(() => fnKeyColumns(this));
                _relatedEntity = new Lazy<MappedEntity>(() => fnRelatedEntity(this));
                _relatedKeyMembers = new Lazy<IReadOnlyList<MappedColumn>>(() => fnRelatedKeyColumns(this));
            }
        }

        protected bool TryGetType(string name, out Type type)
        {
            type = Type.GetType(name);
            
            if (type == null
                && this.ContextType != null)
            {
                type = this.ContextType.Assembly.GetType(name);
            }

            if (type == null)
            {
                // look for type in assemblies that reference the toolkit
                foreach (var assembly in Factories.EntityProviderFactoryRegistry.Singleton.SearchAssemblies)
                {
                    type = assembly.GetType(name);
                    if (type != null)
                        break;
                }
            }

            return type != null;
        }
    }
}
