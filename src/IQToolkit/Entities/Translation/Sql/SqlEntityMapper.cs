// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Entities.Translation
{
    using Expressions;
    using Expressions.Sql;
    using Mapping;
    using System.Diagnostics.CodeAnalysis;
    using Utils;

    /// <summary>
    /// A <see cref="MappingTranslator"/> that maps entitis for <see cref="SqlExpression"/> queries.
    /// </summary>
    public class SqlEntityMapper : MappingTranslator
    {
        public override EntityMapping Mapping { get; }

        public SqlEntityMapper(EntityMapping mapping)
        {
            this.Mapping = mapping;
        }

        /// <summary>
        /// Applies additional mapping related rewrites to the entire query.
        /// </summary>
        public override Expression ApplyMappingRewrites(
            Expression expression,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            // convert references to association properties into correlated queries
            var related = expression.RewriteRelationshipMembers(linguist, this, police);

            // rewrite comparision checks between entities and multi-valued constructs
            var result = related.ConvertEntityComparisons(this.Mapping);

            return result;
        }

        /// <summary>
        /// The query language specific type for the column
        /// </summary>
        public virtual QueryType GetColumnType(
            ColumnMember member,
            QueryLanguage language)
        {
            return GetColumnType(member.Column, language);
        }

        /// <summary>
        /// The query language specific type for the column
        /// </summary>
        public virtual QueryType GetColumnType(
            MappedColumn column,
            QueryLanguage language)
        {
            if (column.Type != null
                && language.TypeSystem.Parse(column.Type) is { } parsedType)
            {
                return parsedType;
            }

            if (column.Member != null)
            {
                return language.TypeSystem.GetQueryType(TypeHelper.GetMemberType(column.Member.Member));
            }

            return QueryType.Unknown;
        }

        public virtual Type GetMemberType(
            ColumnMember member)
        {
            return TypeHelper.GetMemberType(member.Member);
        }

        public virtual Type GetMemberType(
            MappedColumn column)
        {
            return column.Member != null
                ? GetMemberType(column.Member)
                : typeof(string);
        }

        public class ConstructAssignment
        {
            public MemberInfo Member { get; }
            public Expression Expression { get; }

            public ConstructAssignment(MemberInfo member, Expression expression)
            {
                if (member == null)
                    throw new ArgumentNullException(nameof(member));
                if (expression == null)
                    throw new ArgumentNullException(nameof(expression));

                this.Member = member;
                this.Expression = expression;
            }
        }

        protected virtual Expression BuildConstructionExpression(
            Type type, 
            Type constructedType, 
            IReadOnlyList<ConstructAssignment> assignments)
        {
            NewExpression newExpression;

            // handle cases where members are not directly assignable
            var readonlyMembers = assignments.Where(b => TypeHelper.IsReadOnly(b.Member)).ToArray();
            var cons = constructedType.GetDeclaredConstructors();
            var hasNoArgConstructor = cons.Any(c => c.GetParameters().Length == 0);

            if (readonlyMembers.Length > 0 || !hasNoArgConstructor)
            {
                // find all the constructors that bind all the read-only members
                var consThatApply = cons
                    .Select(c => this.BindConstructor(c, readonlyMembers)!)
                    .Where(cbr => cbr != null && cbr.Remaining.Count == 0)
                    .ToList();

                if (consThatApply.Count == 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot construct type '{0}' with all mapped and included members.", constructedType));
                }

                // just use the first one... (Note: need better algorithm?)
                if (readonlyMembers.Length == assignments.Count)
                {
                    return consThatApply[0].Expression;
                }

                var r = this.BindConstructor(consThatApply[0].Expression.Constructor, assignments)!;
                newExpression = r.Expression;
                assignments = r.Remaining;
            }
            else
            {
                newExpression = Expression.New(constructedType);
            }

            Expression result;
            if (assignments.Count > 0)
            {
                if (type.IsInterface)
                {
                    assignments = this.RemapAssignments(assignments, constructedType).ToList();
                }

                result = Expression.MemberInit(
                    newExpression, 
                    (MemberBinding[])assignments.Select(a => Expression.Bind(a.Member, a.Expression))
                    .ToArray());
            }
            else
            {
                result = newExpression;
            }

            if (type != constructedType)
            {
                result = Expression.Convert(result, type);
            }

            return result;
        }

        private IEnumerable<ConstructAssignment> RemapAssignments(IEnumerable<ConstructAssignment> assignments, Type entityType)
        {
            foreach (var assign in assignments)
            {
                if (entityType.TryGetDeclaredFieldOrProperty(assign.Member.Name, out var member))
                {
                    yield return new ConstructAssignment(member, assign.Expression);
                }
                else
                {
                    yield return assign;
                }
            }
        }

        /// <summary>
        /// Attempts to match up entity assignments with constructor parameters.
        /// Returns the <see cref="NewExpression"/> that constructs the entity with the matching assignment values
        /// and the remaining unused assignments.
        /// </summary>
        protected virtual ConstructorBindResult? BindConstructor(
            ConstructorInfo cons,
            IReadOnlyList<ConstructAssignment> assignments)
        {
            var ps = cons.GetParameters();
            var args = new Expression[ps.Length];
            var mis = new MemberInfo[ps.Length];
            var members = new HashSet<ConstructAssignment>(assignments);
            var used = new HashSet<ConstructAssignment>();

            for (int i = 0, n = ps.Length; i < n; i++)
            {
                ParameterInfo p = ps[i];

                var assignment = members.FirstOrDefault(a =>
                    p.Name == a.Member.Name
                    && p.ParameterType.IsAssignableFrom(a.Expression.Type));

                if (assignment == null)
                {
                    assignment = members.FirstOrDefault(a =>
                        string.Compare(p.Name, a.Member.Name, StringComparison.OrdinalIgnoreCase) == 0
                        && p.ParameterType.IsAssignableFrom(a.Expression.Type));
                }

                if (assignment != null)
                {
                    args[i] = assignment.Expression;
                    mis[i] = assignment.Member;
                    used.Add(assignment);
                }
                else
                {
                    // find member with same name as parameter and associate it in object initializer
                    MemberInfo mem = TypeHelper.GetDeclaredFieldsAndProperties(cons.DeclaringType).Where(m => string.Compare(m.Name, p.Name, StringComparison.OrdinalIgnoreCase) == 0).FirstOrDefault();
                    if (mem != null)
                    {
                        args[i] = Expression.Constant(TypeHelper.GetDefault(p.ParameterType), p.ParameterType);
                        mis[i] = mem;
                    }
                    else
                    {
                        // unknown parameter, does not match any member
                        return null;
                    }
                }
            }

            members.ExceptWith(used);

            return new ConstructorBindResult(Expression.New(cons, args, mis), members);
        }

        protected class ConstructorBindResult
        {
            public NewExpression Expression { get; }
            public IReadOnlyList<ConstructAssignment> Remaining { get; }

            public ConstructorBindResult(NewExpression expression, IEnumerable<ConstructAssignment> remaining)
            {
                this.Expression = expression;
                this.Remaining = remaining.ToReadOnly();
            }
        }

        public override bool HasIncludedMembers(Expression entity, QueryPolicy policy)
        {
            if (entity is EntityExpression ex)
            {
                foreach (var mm in ex.Entity.Members)
                {
                    if (policy.IsIncluded(mm.Member))
                        return true;
                }
            }

            return false;
        }

        public override Expression IncludeMembers(
            Expression entity,
            Func<MemberInfo, bool> fnIsIncluded,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            if (entity is EntityExpression ex)
            {
                var assignments = this.GetAssignments(ex.Expression).ToDictionary(ma => ma.Member.Name);
                bool anyAdded = false;

                foreach (var mm in ex.Entity.Members)
                {
                    ConstructAssignment ea;
                    bool okayToInclude = !assignments.TryGetValue(mm.Member.Name, out ea) || IsNullRelationshipAssignment(ex.Entity, ea);
                    if (okayToInclude && fnIsIncluded(mm.Member))
                    {
                        ea = new ConstructAssignment(mm.Member, this.GetMemberExpression(ex.Expression, mm, linguist, police));
                        assignments[mm.Member.Name] = ea;
                        anyAdded = true;
                    }
                }

                if (anyAdded)
                {
                    return new EntityExpression(ex.Entity, 
                        this.BuildConstructionExpression(
                            ex.Entity.Type, 
                            ex.Entity.ConstructedType, 
                            assignments.Values.ToList()
                        ));
                }
            }

            return entity;
        }

        private bool IsNullRelationshipAssignment(MappedEntity entity, ConstructAssignment assignment)
        {
            if (entity.TryGetMember(assignment.Member.Name, out var m)
                && m is RelationshipMember rm)
            {
                if (assignment.Expression is ConstantExpression cex 
                    && cex.Value == null)
                    return true;
            }

            return false;
        }

        private IEnumerable<ConstructAssignment> GetAssignments(Expression newOrMemberInit)
        {
            var assignments = new List<ConstructAssignment>();
            
            if (newOrMemberInit is MemberInitExpression minit)
            {
                assignments.AddRange(minit.Bindings.OfType<MemberAssignment>().Select(a => new ConstructAssignment(a.Member, a.Expression)));
                newOrMemberInit = minit.NewExpression;
            }

            if (newOrMemberInit is NewExpression nex
                && nex.Members != null)
            {
                assignments.AddRange(
                    Enumerable.Range(0, nex.Arguments.Count)
                              .Where(i => nex.Members[i] != null)
                              .Select(i => new ConstructAssignment(nex.Members[i], nex.Arguments[i]))
                              );
            }

            return assignments;
        }

        protected virtual Expression GetInsertResult(
            MappedEntity entity,
            Expression instance,
            LambdaExpression selector,
            Dictionary<MemberInfo, Expression>? map,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            var tableAlias = new TableAlias();
            var tex = new TableExpression(tableAlias, entity, entity.PrimaryTable.Name);
            var aggregator = Aggregator.GetAggregator(selector.Body.Type, typeof(IEnumerable<>).MakeGenericType(selector.Body.Type));

            Expression where;
            DeclarationCommand? genIdCommand = null;
            var generatedIds = entity.PrimaryKeyMembers.Where(pk => pk.Column.IsGenerated).ToList();
            if (generatedIds.Count > 0)
            {
                if (map == null || !generatedIds.Any(m => map.ContainsKey(m.Member)))
                {
                    var localMap = new Dictionary<MemberInfo, Expression>();
                    genIdCommand = this.GetGeneratedIdCommand(entity, generatedIds, localMap, linguist);
                    map = localMap;
                }

                // is this just a retrieval of one generated id member?
                if (selector.Body is MemberExpression mex
                    && entity.TryGetMember(mex.Member.Name, out var member)
                    && member is ColumnMember columnMember
                    && columnMember.Column.IsGenerated)
                {
                    if (genIdCommand != null && genIdCommand.Source != null)
                    {
                        // just use the select from the genIdCommand
                        return new ClientProjectionExpression(
                            genIdCommand.Source,
                            new ColumnExpression(mex.Type, genIdCommand.Variables[0].QueryType, genIdCommand.Source.Alias, genIdCommand.Source.Columns[0].Name),
                            aggregator
                            );
                    }
                    else
                    {
                        TableAlias alias = new TableAlias();
                        var colType = this.GetColumnType(columnMember, linguist.Language);
                        return new ClientProjectionExpression(
                            new SelectExpression(alias, new[] { new ColumnDeclaration("", map[mex.Member], colType) }, null, null),
                            new ColumnExpression(TypeHelper.GetMemberType(mex.Member), colType, alias, ""),
                            aggregator
                            );
                    }
                }

                where = generatedIds.Select((m, i) =>
                    this.GetMemberExpression(tex, m, linguist, police)
                        .Equal(map[m.Member])
                    ).Aggregate((x, y) => x.And(y));
            }
            else
            {
                where = this.GetIdentityCheck(tex, entity, instance, linguist, police);
            }

            var typeProjector = this.GetEntityExpression(tex, entity, linguist, police);
            var selection = selector.Body.Replace(selector.Parameters[0], typeProjector);
            var newAlias = new TableAlias();
            var pc = ColumnProjector.ProjectColumns(linguist, selection, null, newAlias, tableAlias);
            var pe = new ClientProjectionExpression(
                new SelectExpression(newAlias, pc.Columns, tex, where),
                pc.Projector,
                aggregator
                );

            if (genIdCommand != null)
            {
                return new BlockCommand(genIdCommand, pe);
            }

            return pe;
        }

        protected virtual DeclarationCommand GetGeneratedIdCommand(
            MappedEntity entity,
            IReadOnlyList<ColumnMember> generatedPkMembers,
            Dictionary<MemberInfo, Expression> map,
            LanguageTranslator linguist)
        {
            var columns = new List<ColumnDeclaration>();
            var decls = new List<VariableDeclaration>();
            var alias = new TableAlias();

            foreach (var gpkMember in generatedPkMembers)
            {
                var genId = linguist.GetGeneratedIdExpression(gpkMember);
                var name = gpkMember.Member.Name;
                var colType = this.GetColumnType(gpkMember, linguist.Language);

                columns.Add(new ColumnDeclaration(gpkMember.Member.Name, genId, colType));
                decls.Add(new VariableDeclaration(gpkMember.Member.Name, colType, new ColumnExpression(genId.Type, colType, alias, gpkMember.Member.Name)));

                if (map != null)
                {
                    var vex = new VariableExpression(gpkMember.Member.Name, TypeHelper.GetMemberType(gpkMember.Member), colType);
                    map.Add(gpkMember.Member, vex);
                }
            }

            var select = new SelectExpression(alias, columns, null, null);

            return new DeclarationCommand(decls, select);
        }

        protected virtual Expression GetIdentityCheck(
            Expression root,
            MappedEntity entity,
            Expression instance,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            return entity.PrimaryKeyMembers
                .Select(m => this.GetMemberExpression(root, m, linguist, police)
                    .Equal(Expression.MakeMemberAccess(instance, m.Member)))
                .Aggregate((x, y) => x.And(y));
        }

        protected virtual Expression GetEntityExistsTest(
            MappedEntity entity,
            Expression instance,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            var tq = (ClientProjectionExpression)this.GetQueryExpression(entity, linguist, police);
            Expression where = this.GetIdentityCheck(tq.Select, entity, instance, linguist, police);
            return new ExistsSubqueryExpression(new SelectExpression(new TableAlias(), null, tq.Select, where));
        }

        protected virtual Expression GetEntityStateTest(
            MappedEntity entity,
            Expression? instance,
            LambdaExpression updateCheck,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            var tq = (ClientProjectionExpression)this.GetQueryExpression(entity, linguist, police);
            var where = instance != null ? this.GetIdentityCheck(tq.Select, entity, instance, linguist, police) : null;
            var check = updateCheck.Body.Replace(updateCheck.Parameters[0], tq.Projector);
            where = where != null ? where.And(check) : check;
            return new ExistsSubqueryExpression(
                new SelectExpression(new TableAlias(), null, tq.Select, where));
        }

        protected virtual Expression GetUpdateResult(
            MappedEntity entity,
            Expression instance,
            LambdaExpression selector,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            var tq = (ClientProjectionExpression)this.GetQueryExpression(entity, linguist, police);
            var where = this.GetIdentityCheck(tq.Select, entity, instance, linguist, police);
            var selection = selector.Body.Replace(selector.Parameters[0], tq.Projector);
            var newAlias = new TableAlias();
            var pc = ColumnProjector.ProjectColumns(linguist, selection, null, newAlias, tq.Select.Alias);

            return new ClientProjectionExpression(
                new SelectExpression(newAlias, pc.Columns, tq.Select, where),
                pc.Projector,
                Aggregator.GetAggregator(selector.Body.Type, typeof(IEnumerable<>).MakeGenericType(selector.Body.Type))
                );
        }

        public override Expression GetInsertOrUpdateExpression(
            MappedEntity entity,
            Expression instance,
            LambdaExpression? updateCheck,
            LambdaExpression? resultSelector,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            if (updateCheck != null)
            {
                Expression insert = this.GetInsertExpression(entity, instance, resultSelector, linguist, police);
                Expression update = this.GetUpdateExpression(entity, instance, updateCheck, resultSelector, null, linguist, police);
                var check = this.GetEntityExistsTest(entity, instance, linguist, police);
                return new IfCommand(check, update, insert);
            }
            else
            {
                Expression insert = this.GetInsertExpression(entity, instance, resultSelector, linguist, police);
                Expression update = this.GetUpdateExpression(entity, instance, updateCheck, resultSelector, insert, linguist, police);
                return update;
            }
        }

        /// <summary>
        /// Gets a set of related <see cref="MappedTable"/>'s in dependency order.
        /// </summary>
        public virtual IEnumerable<MappedTable> GetDependencyOrderedTables(MappedEntity entity)
        {
            return entity.Tables
                .Sort(t => t is ExtensionTable et
                    && et.RelatedKeyColumns.Count > 0
                    ? new[] { et.RelatedKeyColumns[0].Table }
                    : null
                    );
        }

        public override Expression GetEntityExpression(
            Expression root, 
            MappedEntity entity,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            // must be some complex type constructed from multiple columns
            var assignments = new List<ConstructAssignment>();

            foreach (var mm in entity.Members)
            {
                if (mm is CompoundMember
                    || mm is ColumnMember)
                {
                    var ee = this.GetMemberExpression(root, mm, linguist, police);
                    assignments.Add(new ConstructAssignment(mm.Member, ee));
                }
            }

            return new EntityExpression(
                entity, 
                this.BuildConstructionExpression(
                    entity.Type,
                    entity.ConstructedType, 
                    assignments
                ));
        }

        public override Expression GetMemberExpression(
            Expression root, 
            MappedMember member,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            if (member is CompoundMember multiMember)
            {
                var assignments = new List<ConstructAssignment>();

                foreach (var mm in multiMember.Members)
                {
                    if (mm is CompoundMember
                        || mm is ColumnMember)
                    {
                        var ee = this.GetMemberExpression(root, mm, linguist, police);
                        assignments.Add(new ConstructAssignment(mm.Member, ee));
                    }
                }

                return this.BuildConstructionExpression(
                    multiMember.Type,
                    multiMember.ConstructedType,
                    assignments
                    );
            }
            else if (member is AssociationMember assoc)
            {
                var projection = (ClientProjectionExpression)this.GetQueryExpression(assoc.RelatedEntity, linguist, police);

                // make where clause for joining back to 'root'
                Expression? where = null;

                for (int i = 0, n = assoc.KeyColumns.Count; i < n; i++)
                {
                    var keyColumn = assoc.KeyColumns[i];
                    var relatedKeyColumn = assoc.RelatedKeyColumns[i];

                    var left = keyColumn.Member != null
                        ? this.GetMemberExpression(projection.Projector, keyColumn.Member, linguist, police)
                        : this.TryGetColumnExpression(projection.Projector, keyColumn, linguist, out var leftCol)
                            ? leftCol
                            : null;

                    var right = relatedKeyColumn.Member != null
                        ? this.GetMemberExpression(root, relatedKeyColumn.Member, linguist, police)
                        : this.TryGetColumnExpression(root, relatedKeyColumn, linguist, out var rightCol)
                            ? rightCol
                            : null;

                    if (left != null && right != null)
                    {
                        var equal = left.Equal(right);

                        where = (where != null)
                            ? where.And(equal)
                            : equal;
                    }
                }

                var newAlias = new TableAlias();
                var pc = ColumnProjector.ProjectColumns(linguist, projection.Projector, null, newAlias, projection.Select.Alias);

                var aggregator = Aggregator.GetAggregator(TypeHelper.GetMemberType(member.Member), typeof(IEnumerable<>).MakeGenericType(pc.Projector.Type));
                var result = new ClientProjectionExpression(
                    new SelectExpression(newAlias, pc.Columns, projection.Select, where),
                    pc.Projector, aggregator
                    );

                return police.ApplyEntityPolicy(result, member.Member, linguist, this);
            }
            else if (member is ColumnMember cm)
            {
                if (TryGetColumnExpression(root, cm.Column, linguist, out var columnExpression))
                {
                    return columnExpression;
                }
                else
                {
                    return root.ResolveMemberAccess(cm.Member);
                }
            }
            else
            {
                throw new InvalidOperationException($"Unhandled mapped member type '{member.GetType().Name}' for member '{member.Member.Name}'.");
            }
        }

        private bool TryGetTableAliasForColumn(
            Expression? root, MappedColumn column, [NotNullWhen(true)] out TableAlias alias)
        {
            if (root is TableExpression table
                && table.Name == column.Table.Name)
            {
                alias = table.Alias;
                return true;
            }
            else if (root is JoinExpression join)
            {
                if (this.TryGetTableAliasForColumn(join.Left, column, out alias))
                    return true;
                if (this.TryGetTableAliasForColumn(join.Right, column, out alias))
                    return true;
            }
            else if (root is SelectExpression select
                && TryGetTableAliasForColumn(select.From, column, out var selectAlias))
            {
                var selectColumn = select.Columns.FirstOrDefault(c => c.Name == column.Name);
                if (selectColumn != null)
                {
                    alias = select.Alias;
                    return true;
                }
            }
            else if (root is EntityExpression entityExpr
                && entityExpr.Entity == column.Table.Entity)
            {
                var columnExpr = entityExpr.FindFirstDownOrDefault<ColumnExpression>(e => e.Name == column.Name);
                if (columnExpr != null)
                {
                    alias = columnExpr.Alias;
                    return true;
                }
            }

            alias = default!;
            return false;
        }

        public override bool TryGetColumnExpression(
            Expression root, 
            MappedColumn column, 
            LanguageTranslator linguist,
            [NotNullWhen(true)] out Expression columnExpression)
        {
            if (TryGetTableAliasForColumn(root, column, out var alias))
            {
                var type = this.GetMemberType(column);

                var dbType = !string.IsNullOrEmpty(column.Type)
                    && linguist.Language.TypeSystem.Parse(column.Type) is { } parsedType
                    ? parsedType
                    : linguist.Language.TypeSystem.GetQueryType(type);

                columnExpression = new ColumnExpression(type, dbType, alias, column.Name);
                return true;
            }
            else
            {
                columnExpression = default!;
                return false;
            }
        }

        public override Expression GetQueryExpression(
            MappedEntity entity,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            if (entity.ExtensionTables.Count == 0)
            {
                var tableAlias = new TableAlias();
                var selectAlias = new TableAlias();
                var table = new TableExpression(tableAlias, entity, entity.PrimaryTable.Name);

                var projector = this.GetEntityExpression(table, entity, linguist, police);
                var pc = ColumnProjector.ProjectColumns(linguist, projector, null, selectAlias, tableAlias);

                var proj = new ClientProjectionExpression(
                    new SelectExpression(selectAlias, pc.Columns, table, null),
                    pc.Projector
                    );

                return police.ApplyEntityPolicy(proj, entity.Type, linguist, this);
            }
            else
            {
                var aliases = new Dictionary<string, TableAlias>();
                
                var tex = new TableExpression(new TableAlias(), entity, entity.PrimaryTable.Name);
                aliases.Add(entity.PrimaryTable.Name, tex.Alias);

                Expression source = tex;

                foreach (var table in entity.ExtensionTables)
                {
                    var joinedTableAlias = new TableAlias();
                    aliases.Add(table.Name, joinedTableAlias);

                    aliases.TryGetValue(table.RelatedTable.Name, out var relatedTableAlias);

                    var joinedTex = new TableExpression(joinedTableAlias, entity, table.Name);

                    Expression? cond = null;
                    for (int i = 0, n = table.KeyColumns.Count; i < n; i++)
                    {
                        var keyColumn = table.KeyColumns[i];
                        var relatedKeyColumn = table.RelatedKeyColumns[i];

                        var memberType = GetMemberType(keyColumn);
                        var columnType = GetColumnType(keyColumn, linguist.Language);

                        var relatedColumn = new ColumnExpression(memberType, columnType, relatedTableAlias, keyColumn.Name);
                        var joinedColumn = new ColumnExpression(memberType, columnType, joinedTableAlias, relatedKeyColumn.Name);
                        var eq = joinedColumn.Equal(relatedColumn);

                        cond = (cond != null)
                            ? cond.And(eq)
                            : eq;
                    }

                    source = new JoinExpression(JoinType.SingletonLeftOuterJoin, source, joinedTex, cond);
                }

                var columns = new List<ColumnDeclaration>();
                this.GetColumns(entity.Members, aliases, columns, linguist, police);
                var root = new SelectExpression(new TableAlias(), columns, source, null);
                var existingAliases = aliases.Values.ToArray();

                var projector = this.GetEntityExpression(root, entity, linguist, police);
                var selectAlias = new TableAlias();
                var pc = ColumnProjector.ProjectColumns(linguist, projector, null, selectAlias, root.Alias);
                var proj = new ClientProjectionExpression(
                    new SelectExpression(selectAlias, pc.Columns, root, null),
                    pc.Projector
                    );

                return (ClientProjectionExpression)police.ApplyEntityPolicy(proj, entity.Type, linguist, this);
            }
        }

        private void GetColumns(
            IReadOnlyList<MappedMember> members,
            Dictionary<string, TableAlias> tableNameToAliasMap,
            List<ColumnDeclaration> columns,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            foreach (var mm in members)
            {
                if (mm is CompoundMember mcm)
                {
                    GetColumns(mcm.Members, tableNameToAliasMap, columns, linguist, police);
                }
                else if (mm is ColumnMember cm)
                {
                    tableNameToAliasMap.TryGetValue(cm.Column.Table.Name, out var alias);
                    var colType = this.GetColumnType(cm, linguist.Language);
                    var ce = new ColumnExpression(TypeHelper.GetMemberType(cm.Member), colType, alias, cm.Column.Name);
                    var cd = new ColumnDeclaration(cm.Column.Name, ce, colType);
                    columns.Add(cd);
                }
            }
        }

        public override Expression GetInsertExpression(
            MappedEntity entity, 
            Expression instance, 
            LambdaExpression? selector,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            if (entity.ExtensionTables.Count == 0)
            {
                var tableAlias = new TableAlias();
                var table = new TableExpression(tableAlias, entity, entity.PrimaryTable.Name);
                var assignments = this.GetColumnAssignments(
                    table,
                    instance,
                    entity.Members,
                    m => !m.Column.IsGenerated || m.Column.IsReadOnly,
                    null,
                    linguist,
                    police
                    );

                if (selector != null)
                {
                    return new BlockCommand(
                        new InsertCommand(table, assignments),
                        this.GetInsertResult(entity, instance, selector, null, linguist, police)
                        );
                }

                return new InsertCommand(table, assignments);
            }
            else
            {
                var commands = new List<Expression>();

                var map = this.GetDependentGeneratedColumns(entity);
                var vexMap = new Dictionary<MemberInfo, Expression>();

                foreach (var table in this.GetDependencyOrderedTables(entity))
                {
                    var tableAlias = new TableAlias();
                    var tex = new TableExpression(tableAlias, entity, table.Name);
                    var assignments = this.GetColumnAssignments(
                        tex,
                        instance,
                        entity.Members,
                        m => m.Column.Table.Name == table.Name && !m.Column.IsGenerated,
                        vexMap,
                        linguist,
                        police
                        );

                    var totalAssignments = assignments.Concat(
                        this.GetRelatedColumnAssignments(tex, entity, table, vexMap, linguist, police)
                        );

                    commands.Add(new InsertCommand(tex, totalAssignments));

                    if (map.TryGetValue(table.Name, out var members))
                    {
                        var d = this.GetDependentGeneratedVariableDeclaration(entity, table, members, instance, vexMap, linguist, police);
                        commands.Add(d);
                    }
                }

                if (selector != null)
                {
                    commands.Add(this.GetInsertResult(entity, instance, selector, vexMap, linguist, police));
                }

                return new BlockCommand(commands);
            }
        }

        private Dictionary<string, IReadOnlyList<ColumnMember>> GetDependentGeneratedColumns(
            MappedEntity entity)
        {
            return
                (from xt in entity.ExtensionTables
                group xt by xt.RelatedTable.Name)
                .ToDictionary(
                    g => g.Key,
                    g => entity.GetColumnMembers(g.SelectMany(xt => xt.RelatedKeyColumns).Distinct())
                );
        }

        // make a variable declaration / initialization for dependent generated values
        private CommandExpression GetDependentGeneratedVariableDeclaration(
            MappedEntity entity, 
            MappedTable table, 
            IReadOnlyList<ColumnMember> members, 
            Expression instance, 
            Dictionary<MemberInfo, Expression> map,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            // first make command that retrieves the generated ids if any
            DeclarationCommand? genIdCommand = null;
            
            var generatedPkMembers = entity.PrimaryKeyMembers.Where(pk => pk.Column.IsGenerated)
                .ToReadOnly();

            if (generatedPkMembers.Count > 0)
            {
                genIdCommand = this.GetGeneratedIdCommand(entity, members, map, linguist);

                // if that's all there is then just return the generated ids
                if (members.Count == generatedPkMembers.Count)
                {
                    return genIdCommand;
                }
            }

            // next make command that retrieves the generated members
            // only consider members that were not generated ids
            members = members.Except(generatedPkMembers).ToList();

            var tableAlias = new TableAlias();
            var tex = new TableExpression(tableAlias, entity, table.Name);

            Expression? where = null;
            if (generatedPkMembers.Count > 0)
            {
                where = generatedPkMembers.Select((m, i) =>
                    this.GetMemberExpression(tex, m, linguist, police)
                    .Equal(map[m.Member]))
                    .Aggregate((x, y) => x.And(y));
            }
            else
            {
                where = this.GetIdentityCheck(tex, entity, instance, linguist, police);
            }

            var selectAlias = new TableAlias();
            var columns = new List<ColumnDeclaration>();
            var variables = new List<VariableDeclaration>();

            foreach (var cm in members)
            {
                var col = (ColumnExpression)this.GetMemberExpression(tex, cm, linguist, police);
                columns.Add(new ColumnDeclaration(cm.Column.Name, col, col.QueryType));
                var vcol = new ColumnExpression(col.Type, col.QueryType, selectAlias, col.Name);
                variables.Add(new VariableDeclaration(cm.Member.Name, col.QueryType, vcol));
                map.Add(cm.Member, new VariableExpression(cm.Member.Name, col.Type, col.QueryType));
            }

            var genMembersCommand = new DeclarationCommand(variables, new SelectExpression(selectAlias, columns, tex, where));

            if (genIdCommand != null)
            {
                return new BlockCommand(genIdCommand, genMembersCommand);
            }

            return genMembersCommand;
        }

        private IEnumerable<ColumnAssignment> GetColumnAssignments(
            Expression table,
            Expression instance, 
            IReadOnlyList<MappedMember> members,
            Func<ColumnMember, bool> fnIncludeColumn,
            Dictionary<MemberInfo, Expression>? map,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            foreach (var m in members)
            {
                if (m is ColumnMember cm 
                    && fnIncludeColumn(cm))
                {
                    yield return new ColumnAssignment(
                        (ColumnExpression)this.GetMemberExpression(table, m, linguist, police),
                        this.GetMemberAccess(instance, m.Member, map)
                        );
                }
                else if (m is CompoundMember mcm)
                {
                    var assignments = this.GetColumnAssignments(
                        table,
                        Expression.MakeMemberAccess(instance, mcm.Member),
                        mcm.Members,
                        fnIncludeColumn,
                        map,
                        linguist,
                        police
                        );

                    foreach (var ca in assignments)
                    {
                        yield return ca;
                    }
                }
            }
        }

        private IEnumerable<ColumnAssignment> GetRelatedColumnAssignments(
            Expression expr,
            MappedEntity entity,
            MappedTable table,
            Dictionary<MemberInfo, Expression> map,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            if (table is ExtensionTable exTable)
            {
                foreach(var relatedColumn in exTable.RelatedKeyColumns)
                {
                    if (relatedColumn.Member != null)
                    {
                        var relatedMemberExpression = map[relatedColumn.Member.Member];

                        if (TryGetColumnExpression(expr, relatedColumn, linguist, out var columnExpr))
                        {
                            yield return new ColumnAssignment(
                                (ColumnExpression)columnExpr,
                                relatedMemberExpression
                                );
                        }
                    }
                }
            }
        }

        private Expression GetMemberAccess(
            Expression instance, 
            MemberInfo member, 
            Dictionary<MemberInfo, Expression>? map)
        {
            Expression exp;
            if (map == null || !map.TryGetValue(member, out exp))
            {
                exp = Expression.MakeMemberAccess(instance, member);
            }
            return exp;
        }

        public override Expression GetUpdateExpression(
            MappedEntity entity, 
            Expression instance, 
            LambdaExpression? updateCheck, 
            LambdaExpression? selector, 
            Expression? @else,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            if (entity.ExtensionTables.Count == 0)
            {
                var tableAlias = new TableAlias();
                var table = new TableExpression(tableAlias, entity, entity.PrimaryTable.Name);

                var where = this.GetIdentityCheck(table, entity, instance, linguist, police);

                if (updateCheck != null)
                {
                    var typeProjector = this.GetEntityExpression(table, entity, linguist, police);
                    var pred = updateCheck.Body.Replace(updateCheck.Parameters[0], typeProjector);
                    where = where.And(pred);
                }

                var assignments = this.GetColumnAssignments(
                    table,
                    instance,
                    entity.Members,
                    m => m.Column.IsUpdatable,
                    null,
                    linguist,
                    police
                    );

                var update = new UpdateCommand(table, where, assignments);

                if (selector != null)
                {
                    return new BlockCommand(
                        update,
                        new IfCommand(
                            linguist.GetRowsAffectedExpression(update).GreaterThan(Expression.Constant(0)),
                            this.GetUpdateResult(entity, instance, selector, linguist, police),
                            @else
                            )
                        );
                }
                else if (@else != null)
                {
                    return new BlockCommand(
                        update,
                        new IfCommand(
                            linguist.GetRowsAffectedExpression(update).LessThanOrEqual(Expression.Constant(0)),
                            @else,
                            null
                            )
                        );
                }
                else
                {
                    return update;
                }
            }
            else
            {
                var commands = new List<Expression>();

                foreach (var table in this.GetDependencyOrderedTables(entity))
                {
                    TableExpression tex = new TableExpression(new TableAlias(), entity, table.Name);
                    var assignments = this.GetColumnAssignments(
                        tex,
                        instance,
                        entity.Members,
                        m => m.Column.IsUpdatable,
                        null,
                        linguist,
                        police
                        );

                    var where = this.GetIdentityCheck(tex, entity, instance, linguist, police);
                    commands.Add(new UpdateCommand(tex, where, assignments));
                }

                if (selector != null)
                {
                    commands.Add(
                        new IfCommand(
                            linguist.GetRowsAffectedExpression(commands[commands.Count - 1]).GreaterThan(Expression.Constant(0)),
                            this.GetUpdateResult(entity, instance, selector, linguist, police),
                            @else
                            )
                        );
                }
                else if (@else != null)
                {
                    commands.Add(
                        new IfCommand(
                            linguist.GetRowsAffectedExpression(commands[commands.Count - 1]).LessThanOrEqual(Expression.Constant(0)),
                            @else,
                            null
                            )
                        );
                }

                Expression block = new BlockCommand(commands);

                if (updateCheck != null)
                {
                    var test = this.GetEntityStateTest(entity, instance, updateCheck, linguist, police);
                    return new IfCommand(test, block, null);
                }

                return block;
            }
        }

        private Expression? GetIdentityCheck(
            TableExpression root, 
            MappedEntity entity, 
            Expression instance, 
            MappedTable table,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            if (table is ExtensionTable exTable)
            {
                var keyColumns = exTable.KeyColumns;
                var relatedKeyColumns = exTable.RelatedKeyColumns;

                Expression? where = null;
                for (int i = 0, n = keyColumns.Count; i < n; i++)
                {
                    var keyColumn = keyColumns[i];
                    var relatedKeyColumn = relatedKeyColumns[i];

                    var relatedMemberType = GetMemberType(relatedKeyColumn);
                    var relatedColumnType = GetColumnType(relatedKeyColumn, linguist.Language);

                    var cex = new ColumnExpression(
                        relatedMemberType, 
                        relatedColumnType, 
                        root.Alias, 
                        keyColumn.Name
                        );

                    var nex = this.GetMemberExpression(instance, relatedKeyColumn.Member!, linguist, police);
                    var eq = cex.Equal(nex);
                    where = (where != null) 
                        ? where.And(eq) 
                        : where;
                }

                return where;
            }
            else
            {
                return entity.PrimaryKeyMembers
                    .Select(m => this.GetMemberExpression(root, m, linguist, police)
                        .Equal(Expression.MakeMemberAccess(instance, m.Member)))
                    .Aggregate((x, y) => x.And(y));
            }
        }

        public override Expression GetDeleteExpression(
            MappedEntity entity, 
            Expression? instance, 
            LambdaExpression? deleteCheck,
            LanguageTranslator linguist,
            PolicyTranslator police)
        {
            if (entity.ExtensionTables.Count == 0)
            {
                var table = new TableExpression(new TableAlias(), entity, entity.PrimaryTable.Name);
                Expression? where = null;

                if (instance != null)
                {
                    where = this.GetIdentityCheck(table, entity, instance, linguist, police);
                }

                if (deleteCheck != null)
                {
                    var row = this.GetEntityExpression(table, entity, linguist, police);
                    var pred = deleteCheck.Body.Replace(deleteCheck.Parameters[0], row);
                    where = (where != null) ? where.And(pred) : pred;
                }

                return new DeleteCommand(table, where);
            }
            else
            {
                if (instance != null)
                {
                    var commands = new List<Expression>();

                    foreach (var table in this.GetDependencyOrderedTables(entity).Reverse())
                    {
                        var tex = new TableExpression(new TableAlias(), entity, table.Name);
                        var where = this.GetIdentityCheck(tex, entity, instance, linguist, police);
                        commands.Add(new DeleteCommand(tex, where));
                    }

                    var block = new BlockCommand(commands);

                    if (deleteCheck != null)
                    {
                        var test = this.GetEntityStateTest(entity, instance, deleteCheck, linguist, police);
                        return new IfCommand(test, block, null);
                    }

                    return block;
                }
                else
                {
                    var commands = new List<Expression>();

                    foreach (var table in this.GetDependencyOrderedTables(entity).Reverse())
                    {
                        var tex = new TableExpression(new TableAlias(), entity, table.Name);
                        var where = deleteCheck != null
                            ? this.GetEntityStateTest(entity, null, deleteCheck, linguist, police)
                            : null;
                        commands.Add(new DeleteCommand(tex, where));
                    }

                    return new BlockCommand(commands);
                }
            }
        }
    }
}