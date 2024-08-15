// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace IQToolkit.Entities.Mapping
{
    using System.Collections.Immutable;
    using System.Diagnostics.CodeAnalysis;
    using System.Reflection;
    using Utils;

#if false
    <Mapping>
        <Entity Id="EC" Type="Customer" ConstructedType="Customer" >
            <ColumnMember Name="CustomerID" Column="CustomerID" Table="Customers" />
            <CompoundMember Name="Address" Type="IAddress" ConstructedType="Address">
                <Member Name="Street" Column="Address" Table="..." />
                <Member Name="City" Column="City" Table="..." />
            </CompoundMember>
            <AssociationMember Name="Orders"
                Table="Customers"
                KeyColumns="CustomerID" 
                RelatedEntityId="EO"
                RelatedTable="Orders"
                RelatedKeyColumns="CustomerID",
                IsForeignKey="false"
                />
            <Table Name="Customers">
                <Column Name="CustomerID" DbType="NVARCHAR" IsPrimaryKey="true" />
                <Column Name="ContactName" DbType="NVARCHAR" />              
                <Column Name="City" />
            </Table>
            <ExtensionTable Name="..." KeyColumns="" RelatedTable="" RelatedKeyColumns="">
                <Column Name=".." />
            </ExtensionTable>
        </Entity>
        <Entity Id="EO" Type="Order" RuntimeType="Order>
            <ColumnMember Name="OrderID" Column="OrderID" Table="Orders" />
            <AssociationMember Name="Customer"
                KeyColumns="CustomerID" 
                IsForeignKey="true" // KeyColumns are a foreign key to customers table (important for insert/update order)
                RelatedEntityId="EC"
                RelatedTable="Customers"
                RelatedKeyColumns="CustomerID"
                />
            <Table Name="Orders">
                <Column Name="OrderID" DbType="INT" IsPrimaryKey="true" />
                <Column Name="CustomerID" DbType="NVARCHAR" />
            </Table>
        </Entity>
    </Mapping>

#endif

    /// <summary>
    /// A <see cref="EntityMapping"/> stored in XML elements.
    /// </summary>
    public class XmlMapping : StandardMapping
    {
        private readonly Dictionary<string, XElement> _entityIdToElementsMap;
        private ImmutableDictionary<MemberInfo, string> _contextMembertoEntityIdMap;
        private ImmutableDictionary<string, MemberInfo> _entityIdToContextMemberMap;

        /// <summary>
        /// Constructs a new instance of <see cref="XmlMapping"/>
        /// </summary>
        public XmlMapping(string xml, Type? contextType = null)
            : base(contextType)
        {
            _entityIdToElementsMap = Deserialize(xml);
            _contextMembertoEntityIdMap = ImmutableDictionary<MemberInfo, string>.Empty;
            _entityIdToContextMemberMap = ImmutableDictionary<string, MemberInfo>.Empty;

            if (contextType != null)
                this.InitializeContextMembers();
        }

        private Dictionary<string, XElement> Deserialize(string xml)
        {
            return XElement.Parse(xml)
                .Elements(EntityName)
                .Where(e => GetId(e) != null)
                .ToDictionary(GetId)!;
        }

        protected override string GetEntityId(Type entityType)
        {
            if (this.ContextType != null
                && this.TryGetContextMember(entityType, out var contextMember))
            {
                return GetEntityId(contextMember);
            }

            // use the entity type name as the entity id
            return entityType.Name;
        }

        protected override string GetEntityId(MemberInfo contextMember)
        {
            if (contextMember is Type type)
                return GetEntityId(type);

            if (!_contextMembertoEntityIdMap.TryGetValue(contextMember, out var id))
            {
                var contextMemberType = TypeHelper.GetEntityType(contextMember);
                if (!TryGetEntityIdFromContextMemberEntityType(contextMemberType, out id))
                {
                    id = contextMember.Name;
                }

                id = ImmutableInterlocked.GetOrAdd(ref _contextMembertoEntityIdMap, contextMember, id);
            }

            return id;
        }

        private bool TryGetEntityIdFromContextMemberEntityType(
            Type entityType, 
            [NotNullWhen(true)] out string? entityId)
        {
            foreach (var kvp in _entityIdToElementsMap)
            {
                var typeInElement = GetEntityType(kvp.Value);
                if (typeInElement == entityType.Name 
                    || typeInElement == entityType.FullName)
                {
                    entityId = kvp.Key;
                    return true;
                }
            }

            entityId = null;
            return false;
        }

        protected override MappedEntity CreateEntity(
            Type entityType, string entityId)
        {
            _entityIdToElementsMap.TryGetValue(entityId, out var entityElement);
            return CreateEntity(entityType, entityId, entityElement);
        }

        private MappedEntity CreateEntity(
            Type entityType, string entityId, XElement? entityElement)
        {
            var constructedType = GetConstructedType(entityElement) is { } typeName && this.TryGetType(typeName, out var typeFromElement)
                ? typeFromElement
                : entityType;

            // build map of all members to their mapping elements
            Dictionary<MemberInfo, XElement>? memberToElementMap = null;
            if (entityElement != null)
            {
                memberToElementMap = new Dictionary<MemberInfo, XElement>();
                GetMemberElements(entityType, entityElement, memberToElementMap);
            }
                
            return new StandardEntity(
                this,
                entityId,
                entityType,
                constructedType,
                me => CreateEntityTables(me, entityElement, memberToElementMap),
                me => CreateMembers(me, null, memberToElementMap)
                );
        }

        private void GetMemberElements(
            Type type,
            XElement element,
            Dictionary<MemberInfo, XElement> map)
        {
            foreach (var prop in element.Elements().Where(IsMember))
            {
                if (GetName(prop) is { } name 
                    && type.TryGetDeclaredFieldOrProperty(name, out var member))
                {
                    map[member] = prop;

                    var memberType = TypeHelper.GetMemberType(member);
                    GetMemberElements(memberType, prop, map);
                }
            }
        }

        protected virtual IReadOnlyList<MappedTable> CreateEntityTables(
            MappedEntity entity, 
            XElement? entityElement,
            Dictionary<MemberInfo, XElement>? memberToElementMap)
        {
            var tableElement = entityElement?.Element(TableName);
            var extTableElements = entityElement?.Elements(ExtendedTableName);

            var tables = new List<MappedTable>();

            var primaryTable = CreateTable(entity, entityElement, tableElement, memberToElementMap);
            tables.Add(primaryTable);

            if (extTableElements != null)
            {
                foreach (var extTableElement in extTableElements)
                {
                    var extTable = CreateTable(entity, entityElement, extTableElement, memberToElementMap);
                    tables.Add(extTable);
                }
            }

            return tables.ToReadOnly();
        }

        protected virtual IReadOnlyList<MappedMember> CreateMembers(
            MappedEntity entity,
            MappedMember? parent,
            Dictionary<MemberInfo, XElement>? memberToElementMap)
        {
            var declaringType = parent != null
                ? TypeHelper.GetSequenceElementType(parent.Type)
                : entity.Type;

            var mappedMembers = new List<MappedMember>();

            foreach (var member in declaringType.GetDeclaredFieldsAndProperties())
            {
                // find mapping info for member
                XElement? memberElement = null;
                memberToElementMap?.TryGetValue(member, out memberElement);

                if (TryCreateMember(entity, parent, member, memberElement, memberToElementMap, out var mappedMember))
                {
                    mappedMembers.Add(mappedMember);
                }
            }

            return mappedMembers.ToReadOnly();
        }

        protected virtual bool TryCreateMember(
            MappedEntity entity,
            MappedMember? parent,
            MemberInfo member,
            XElement? memberElement,
            Dictionary<MemberInfo, XElement>? memberToElementMap,
            [NotNullWhen(true)] out MappedMember? mappedMember)
        {
            // associations have key columns
            if (memberElement != null)
            {
                if (IsAssociationMember(memberElement)
                    && GetKeyColumns(memberElement) is { } keyColumns)
                {
                    var isForeignKey = GetIsForeignKey(memberElement);

                    mappedMember = new StandardAssociationMember(
                        entity,
                        parent,
                        member,
                        isForeignKey,
                        fnKeyColumns: me =>
                            this.GetEntityColumns(
                                entity,
                                keyColumns,
                                GetTableName(memberElement)
                                ),
                        fnRelatedEntity: me =>
                        {
                            var relatedEntityType = TypeHelper.GetSequenceElementType(TypeHelper.GetMemberType(member));
                            var relatedEntityId = GetRelatedEntityId(memberElement) is { } relatedEntityIdElement
                                ? (string)relatedEntityIdElement
                                : this.GetEntityId(relatedEntityType);
                            return this.GetEntity(relatedEntityType, relatedEntityId);
                        },
                        fnRelatedKeyColumns: me =>
                            this.GetEntityColumns(
                                entity,
                                GetRelatedKeyColumns(memberElement) ?? keyColumns,
                                GetRelatedTableName(memberElement)
                                )
                        );

                    return true;
                }
                // compound members have nested property elements
                else if (IsCompoundMember(memberElement))
                {
                    var constructedType = GetConstructedType(memberElement) is string typeName
                        && this.TryGetType(typeName, out var typeFromElement)
                        ? typeFromElement
                        : TypeHelper.GetEntityType(member);

                    mappedMember = new StandardCompoundMember(
                        entity,
                        parent,
                        member,
                        constructedType,
                        me => this.CreateMembers(entity, me, memberToElementMap)
                        );

                    return true;
                }
                else if (IsColumnMember(memberElement))
                {
                    // anything else must be a single column member
                    var memberType = TypeHelper.GetMemberType(member);

                    mappedMember = new StandardColumnMember(
                        entity,
                        parent,
                        member,
                        me =>
                        {
                            me.Entity.TryGetColumn(
                                GetColumnName(memberElement) ?? member.Name,
                                GetTableName(memberElement),
                                out var column
                                );
                            System.Diagnostics.Debug.Assert(column != null);
                            return column!;
                        });

                    return true;
                }
            }
            else if (IsPossibleColumnMember(member))
            {
                // infer this member to be mapped to a column of the same name
                var memberType = TypeHelper.GetMemberType(member);
                mappedMember = new StandardColumnMember(
                    entity,
                    parent,
                    member,
                    me =>
                    {
                        me.Entity.TryGetColumn(
                            member.Name,
                            entity.PrimaryTable.Name,
                            out var column
                            );
                        System.Diagnostics.Debug.Assert(column != null);
                        return column!;
                    });

                return true;
            }
            else if (IsPossibleCompoundMember(member))
            {
                // infer this member to be a compound member
                mappedMember = new StandardCompoundMember(
                    entity,
                    parent,
                    member,
                    TypeHelper.GetMemberType(member),
                    me => this.CreateMembers(entity, me, memberToElementMap)
                    );

                return true;
            }

            mappedMember = null;
            return false;
        }

        private static readonly char[] _nameListSeparators = new char[] { ' ', ',', '|' };

        private MappedTable CreateTable(
            MappedEntity entity, 
            XElement? entityElement,
            XElement? tableElement,
            Dictionary<MemberInfo, XElement>? memberToElementMap)
        {
            _entityIdToContextMemberMap.TryGetValue(entity.Id, out var contextMember);

            var tableName = GetName(tableElement) 
                ?? contextMember?.Name 
                ?? entity.Type.Name;

            if (tableElement?.Name == ExtendedTableName)
            {
                return new StandardExtensionTable(
                    entity,
                    tableName,
                    me => CreateTableColumns(me, entityElement, tableElement, memberToElementMap),
                    me => this.GetTableColumns(me, GetKeyColumns(tableElement) ?? ""),
                    () => GetRelatedTableName(tableElement) is { } relatedTableName 
                        && entity.TryGetTable(relatedTableName, out var relatedTable)
                            ? relatedTable
                            : entity.PrimaryTable,
                    me => this.GetTableColumns(
                            me.RelatedTable, 
                            GetRelatedKeyColumns(tableElement) ?? GetKeyColumns(tableElement) ?? ""
                            )
                    );
            }
            else
            {
                return new StandardPrimaryTable(
                    entity,
                    tableName,
                    me => CreateTableColumns(me, entityElement, tableElement, memberToElementMap)
                    );
            }
        }

        private IReadOnlyList<MappedColumn> CreateTableColumns(
            MappedTable table, 
            XElement? entityElement,
            XElement? tableElement,
            Dictionary<MemberInfo, XElement>? memberToElementMap)
        {
            // all the columns declared as part of the table
            // + any additional columns referenced in a property
            // + any additional columns referenced in an extension table
            // + all the unmapped members (if primary table)

            var columns = new List<MappedColumn>();
            var declared = new HashSet<string>();

            // all columns declared in table element
            if (tableElement != null)
            {
                foreach (var colElement in GetColumns(tableElement))
                {
                    if (TryCreateColumn(table, colElement, out var column))
                    {
                        if (declared.Add(column.Name))
                        {
                            columns.Add(column);
                        }
                    }
                }
            }

            if (entityElement != null)
            {
                // any additional columns for this table referenced in a declared property
                AddInferredElementColumns(table, entityElement, columns, memberToElementMap, declared);
            }

            // any additional columns inferred from entity type itself
            if (table == table.Entity.PrimaryTable)
            {
                AddInferredMemberColumns(table, table.Entity.Type, columns, memberToElementMap, declared);
            }

            return columns.ToReadOnly();
        }

        private void AddInferredElementColumns(
            MappedTable table,
            XElement element,
            List<MappedColumn> columns,
            Dictionary<MemberInfo, XElement>? memberToElementMap,
            HashSet<string> declared)
        {
            foreach (var member in GetMembers(element))
            {
                // table name may be implied
                var tableName = GetTableName(member)
                    ?? (table == table.Entity.PrimaryTable ? table.Name : null);

                if (IsColumnMember(member)
                    && tableName == table.Name)
                {
                    var columnName = GetColumnName(member)
                        ?? GetName(member);

                    if (columnName != null
                        && declared.Add(columnName))
                    {
                        columns.Add(CreateColumn(table, columnName));
                    }
                }
                else if (IsCompoundMember(member))
                {
                    AddInferredElementColumns(table, member, columns, memberToElementMap, declared);
                }
                else if (IsAssociationMember(member))
                {
                    if (tableName == table.Name
                        && GetKeyColumns(member) is { } keyColumns)
                    {
                        foreach (var keyColumnName in this.GetNames(keyColumns))
                        {
                            if (declared.Add(keyColumnName))
                            {
                                columns.Add(CreateColumn(table, keyColumnName));
                            }
                        }
                    }
                    else if (
                        GetRelatedTableName(member) == table.Name
                        && GetRelatedKeyColumns(member) is { } relatedKeyColumns)
                    {
                        foreach (var relatedKeyColumnName in this.GetNames(relatedKeyColumns))
                        {
                            if (declared.Add(relatedKeyColumnName))
                            {
                                columns.Add(CreateColumn(table, relatedKeyColumnName));
                            }
                        }
                    }
                }
            }
        }

        private void AddInferredMemberColumns(
            MappedTable table,
            Type type, 
            List<MappedColumn> columns, 
            Dictionary<MemberInfo, XElement>? memberToElementMap,
            HashSet<string> declared)
        {
            var members = type.GetDeclaredFieldsAndProperties();

            foreach (var member in members)
            {
                XElement? memberElement = null;
                memberToElementMap?.TryGetValue(member, out memberElement);

                if (memberElement != null)
                {
                    if (IsCompoundMember(memberElement))
                    {
                        // continue on to nested members, since they may not have declarations
                        var mcType = TypeHelper.GetMemberType(member);
                        AddInferredMemberColumns(table, mcType, columns, memberToElementMap, declared);
                    }
                    else
                    {
                        // do nothing, column is already inferred from element
                    }
                }
                else if (IsPossibleColumnMember(member))
                {
                    if (declared.Add(member.Name))
                    {
                        columns.Add(CreateColumn(table, member.Name));
                    }
                }
                else if (IsPossibleCompoundMember(member))
                {
                    var mcType = TypeHelper.GetMemberType(member);
                    AddInferredMemberColumns(table, mcType, columns, memberToElementMap, declared);
                }
            }
        }

        private bool TryCreateColumn(
            MappedTable table, 
            XElement columnElement, 
            [NotNullWhen(true)] out MappedColumn column)
        {
            var columnName = GetName(columnElement);
            if (columnName != null)
            {
                var columnType = GetColumnType(columnElement);
                var isPrimaryKey = GetIsPrimaryKey(columnElement);
                var isReadOnly = GetIsReadOnly(columnElement);
                var isComputed = GetIsComputed(columnElement);
                var isGenerated = GetIsGenerated(columnElement);

                column = new StandardColumn(
                    table,
                    columnName,
                    columnType,
                    isPrimaryKey: isPrimaryKey,
                    isReadOnly: isReadOnly,
                    isComputed: isComputed,
                    isGenerated: isGenerated,
                    me => table.Entity.Members.OfType<ColumnMember>().FirstOrDefault(cm => cm.Column == me)
                    );

                return true;
            }

            column = default!;
            return false;
        }

        private MappedColumn CreateColumn(
            MappedTable table, 
            string name)
        {
            return new StandardColumn(
                table,
                name,
                columnType: null,
                isPrimaryKey: false,
                isReadOnly: false,
                isComputed: false,
                isGenerated: false,
                me => table.Entity.Members.OfType<ColumnMember>().FirstOrDefault(cm => cm.Column == me)
                );
        }

        /// <summary>
        /// Converts the XML mapping text to an <see cref="EntityMapping"/>.
        /// </summary>
        public static EntityMapping FromXml(string xml, Type? contextType = null)
        {
            return new XmlMapping(xml, contextType);
        }

        /// <summary>
        /// Converts <see cref="EntityMapping"/> to serialized XML text.
        /// </summary>
        public static string ToXml(EntityMapping mapping, bool minimal=true)
        {
            var mappingElement = new XElement("Mapping");

            foreach (var entity in mapping.GetEntities().OrderBy(e => e.Id))
            {
                var entityElement = ToEntityElement(entity, minimal);
                mappingElement.Add(entityElement);
            }

            return mappingElement.ToString();
        }

        private static XElement ToEntityElement(MappedEntity entity, bool minimal)
        {
            var element = new XElement(EntityName);

            element.Add(new XAttribute(IdName, entity.Id));
            element.Add(new XAttribute(TypeName, entity.Type.FullName));

            if (!minimal || entity.ConstructedType != entity.Type)
                element.Add(new XAttribute(ConstructedTypeName, entity.ConstructedType.FullName));

            foreach (var member in entity.Members.OrderBy(m => m.Member.Name))
            {
                if (member is ColumnMember cm 
                    && cm.Member.Name == cm.Column.Name
                    && minimal)
                    continue;

                element.Add(ToMemberElement(member, minimal));
            }

            element.Add(ToTableElement(entity.PrimaryTable, minimal));

            foreach (var table in entity.ExtensionTables.OrderBy(t => t.Name))
            {
                element.Add(ToTableElement(table, minimal));
            }

            return element;
        }

        private static XElement ToTableElement(MappedTable table, bool minimal)
        {
            if (table is ExtensionTable extTable)
            {
                var element = new XElement(ExtendedTableName);

                element.Add(new XAttribute(NameName, table.Name));
                element.Add(new XAttribute(KeyColumnsName, GetColumnNames(extTable.KeyColumns)));
                element.Add(new XAttribute(RelatedTableName, extTable.RelatedTable.Name));
                element.Add(new XAttribute(RelatedKeyColumnsName, GetColumnNames(extTable.RelatedKeyColumns)));

                foreach (var column in table.Columns.OrderBy(c => c.Name))
                {
                    element.Add(ToColumnElement(column, minimal));
                }

                return element;
            }
            else
            {
                var element = new XElement(TableName);

                element.Add(new XAttribute(NameName, table.Name));

                foreach (var column in table.Columns.OrderBy(c => c.Name))
                {
                    element.Add(ToColumnElement(column, minimal));
                }

                return element;
            }
        }

        private static bool CanBeInferred(MappedColumn column)
        {
            return column.Member != null
                && column.Member.Member.Name == column.Name
                && column.Type == null
                && column.IsPrimaryKey == false
                && column.IsReadOnly == false
                && column.IsComputed == false
                && column.IsGenerated == false;
        }

        private static XElement ToColumnElement(MappedColumn column, bool minimal)
        {
            var element = new XElement(ColumnName);
            element.Add(new XAttribute(NameName, column.Name));
            if (!minimal || !string.IsNullOrEmpty(column.Type))
                element.Add(new XAttribute(TypeName, column.Type ?? ""));
            if (!minimal || column.IsPrimaryKey)
                element.Add(new XAttribute(IsPrimaryKeyName, column.IsPrimaryKey));
            if (!minimal || column.IsReadOnly)
                element.Add(new XAttribute(IsReadOnlyName, column.IsReadOnly));
            if (!minimal || column.IsComputed)
                element.Add(new XAttribute(IsComputedName, column.IsComputed));
            if (!minimal || column.IsGenerated)
                element.Add(new XAttribute(IsGeneratedName, column.IsGenerated));
            return element;
        }

        private static XElement ToMemberElement(MappedMember member, bool minimal)
        {
            switch (member)
            {
                case ColumnMember cm:
                    {
                        var element = new XElement(ColumnMemberName);
                        element.Add(new XAttribute(NameName, member.Member.Name));

                        if (!minimal || cm.Column.Name != cm.Member.Name)
                            element.Add(new XAttribute(ColumnName, cm.Column.Name));
                        if (!minimal || cm.Column.Table != cm.Column.Table.Entity.PrimaryTable)
                            element.Add(new XAttribute(TableName, cm.Column.Table.Name));
                        return element;
                    }

                case CompoundMember cm:
                    {
                        var element = new XElement(CompoundMemberName);
                        element.Add(new XAttribute(NameName, member.Member.Name));

                        if (!minimal || cm.ConstructedType != cm.Type)
                            element.Add(new XAttribute(ConstructedTypeName, cm.ConstructedType.FullName));

                        foreach (var cmm in cm.Members.OrderBy(m => m.Member.Name))
                        {
                            if (cmm is ColumnMember colMember
                                && colMember.Member.Name == colMember.Column.Name
                                && minimal)
                                continue;

                            element.Add(ToMemberElement(cmm, minimal));
                        }

                        return element;
                    }

                case AssociationMember am:
                    {
                        var element = new XElement(AssociationMemberName);
                        element.Add(new XAttribute(NameName, member.Member.Name));

                        var keyNames = GetColumnNames(am.KeyColumns);
                        element.Add(new XAttribute(KeyColumnsName, keyNames));
                        element.Add(new XAttribute(RelatedEntityIdName, am.RelatedEntity.Id));

                        var relatedNames = GetColumnNames(am.RelatedKeyColumns);
                        if (!minimal || relatedNames != keyNames)
                            element.Add(new XAttribute(RelatedKeyColumnsName, relatedNames));
                        
                        if (!minimal || am.IsSource)
                            element.Add(new XAttribute(IsForeignKeyName, am.IsSource));

                        return element;
                    }

                default:
                    throw new InvalidCastException($"{nameof(XmlMapping)}: Unhandled member type '{member.GetType().Name}' in {nameof(ToMemberElement)}");
            }
        }

        private static string GetColumnNames(IEnumerable<MappedColumn> columns)
        {
            return string.Join(", ", columns.Select(c => c.Name));
        }

        private Type? GetType(string? typeName) =>
            typeName != null
                && this.TryGetType(typeName, out var type)
                ? type
                : null;

        private bool IsMember(XElement element) =>
            IsColumnMember(element)
            || IsCompoundMember(element)
            || IsAssociationMember(element);

        private bool IsColumnMember(XElement element) =>
            element.Name == ColumnMemberName;

        private bool IsAssociationMember(XElement element) =>
            element.Name == AssociationMemberName;

        private bool IsCompoundMember(XElement element) =>
            element.Name == CompoundMemberName;

        private string? GetId(XElement? element) =>
            element?.Attribute(IdName)?.Value;

        private string? GetEntityType(XElement? element) =>
            element?.Attribute(TypeName)?.Value;

        private string? GetConstructedType(XElement? element) =>
            element?.Attribute(ConstructedTypeName)?.Value;

        private string? GetName(XElement? element) =>
            element?.Attribute(NameName)?.Value;

        private string? GetColumnName(XElement? element) =>
            element?.Attribute(ColumnName)?.Value;

        private string? GetTableName(XElement? element) =>
            element?.Attribute(TableName)?.Value;

        private string? GetColumnType(XElement? element) =>
            element?.Attribute(TypeName)?.Value;

        private bool GetIsPrimaryKey(XElement? element) =>
            ((bool?)element?.Attribute(IsPrimaryKeyName)) ?? false;

        private bool GetIsComputed(XElement? element) =>
            ((bool?)element?.Attribute(IsComputedName)) ?? false;

        private bool GetIsGenerated(XElement? element) =>
            ((bool?)element?.Attribute(IsGeneratedName)) ?? false;

        private bool GetIsReadOnly(XElement? element) =>
            ((bool?)element?.Attribute(IsReadOnlyName)) ?? false;

        private string? GetKeyColumns(XElement? element) =>
            element?.Attribute(KeyColumnsName)?.Value;

        private string? GetRelatedTableName(XElement? element) =>
            element?.Attribute(RelatedTableName)?.Value;

        private string? GetRelatedKeyColumns(XElement? element) =>
            element?.Attribute(RelatedKeyColumnsName)?.Value;

        private string? GetRelatedEntityId(XElement? element) =>
            element?.Attribute(RelatedEntityIdName)?.Value;

        private bool GetIsForeignKey(XElement? element) =>
            ((bool?)element?.Attribute(IsForeignKeyName)) ?? false;

        private IEnumerable<XElement> GetMembers(XElement? element) =>
            element?.Elements().Where(IsMember) ?? ReadOnlyList<XElement>.Empty;

        private IEnumerable<XElement> GetTables(XElement? element) =>
            element?.Elements(TableName) ?? ReadOnlyList<XElement>.Empty;

        private IEnumerable<XElement> GetColumns(XElement element) =>
            element?.Elements(ColumnName) ?? ReadOnlyList<XElement>.Empty;


        private static readonly XName EntityName = XName.Get("Entity");
        private static readonly XName TypeName = XName.Get("Type");
        private static readonly XName ConstructedTypeName = XName.Get("ConstructedType");
        private static readonly XName IdName = XName.Get("Id");

        private static readonly XName TableName = XName.Get("Table");
        private static readonly XName NameName = XName.Get("Name");
        private static readonly XName ExtendedTableName = XName.Get("ExtendedTable");
        private static readonly XName KeyColumnsName = XName.Get("KeyColumns");
        private static readonly XName RelatedTableName = XName.Get("RelatedTable");
        private static readonly XName RelatedKeyColumnsName = XName.Get("RelatedKeyColumns");

        private static readonly XName MemberName = XName.Get("Member");
        private static readonly XName ColumnMemberName = XName.Get("ColumnMember");
        private static readonly XName CompoundMemberName = XName.Get("CompoundMember");
        private static readonly XName AssociationMemberName = XName.Get("AssociationMember");

        private static readonly XName ColumnName = XName.Get("Column");
        private static readonly XName IsComputedName = XName.Get("IsComputed");
        private static readonly XName IsPrimaryKeyName = XName.Get("IsPrimaryKey");
        private static readonly XName IsGeneratedName = XName.Get("IsGenerated");
        private static readonly XName IsReadOnlyName = XName.Get("IsReadOnly");

        private static readonly XName RelatedEntityIdName = XName.Get("RelatedEntityId");
        private static readonly XName IsForeignKeyName = XName.Get("IsForeignKey");
    }
}