﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace IQToolkit.Entities.Mapping
{
    using Utils;

#if false
    /// <summary>
    /// A simple query mapping that attempts to infer mapping from naming conventions
    /// </summary>
    public class ImplicitEntityMapping : BasicEntityMapping
    {
        public ImplicitEntityMapping()
        {
        }

        public override string GetEntityId(Type type)
        {
            return this.InferTableName(type);
        }

        public override bool IsPrimaryKey(MappedEntity entity, MemberInfo member)
        {
            // Customers has CustomerID, Orders has OrderID, etc
            if (this.IsColumn(entity, member)) 
            {
                string name = NameWithoutTrailingDigits(member.Name);
                return member.Name.EndsWith("ID") && member.DeclaringType.Name.StartsWith(member.Name.Substring(0, member.Name.Length - 2)); 
            }
            return false;
        }

        private string NameWithoutTrailingDigits(string name)
        {
            int n = name.Length - 1;
            while (n >= 0 && char.IsDigit(name[n]))
            {
                n--;
            }
            if (n < name.Length - 1)
            {
                return name.Substring(0, n);
            }
            return name;
        }

        public override bool IsColumn(MappedEntity entity, MemberInfo member)
        {
            return IsScalar(TypeHelper.GetMemberType(member));
        }

        private static bool IsScalar(Type type)
        {
            type = TypeHelper.GetNonNullableType(type);
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Empty:
                    return false;
                case TypeCode.Object:
                    return
                        type == typeof(DateTimeOffset) ||
                        type == typeof(TimeSpan) ||
                        type == typeof(Guid) ||
                        type == typeof(byte[]);
                default:
                    return true;
            }
        }

        public override bool IsAssociationRelationship(MappedEntity entity, MemberInfo member)
        {
            if (IsMapped(entity, member) && !IsColumn(entity, member))
            {
                Type otherType = TypeHelper.GetSequenceElementType(TypeHelper.GetMemberType(member));
                return !IsScalar(otherType);
            }
            return false;
        }

        public override bool IsRelationshipSource(MappedEntity entity, MemberInfo member)
        {
            if (IsAssociationRelationship(entity, member))
            {
                if (typeof(IEnumerable).IsAssignableFrom(TypeHelper.GetMemberType(member)))
                    return false;

                // is source of relationship if relatedKeyMembers are the related entity's primary keys
                MappedEntity entity2 = GetRelatedEntity(entity, member);
                var relatedPKs = new HashSet<string>(this.GetPrimaryKeyMembers(entity2).Select(m => m.Name));
                var relatedKeyMembers = new HashSet<string>(this.GetAssociationRelatedKeyMembers(entity, member).Select(m => m.Name));
                return relatedPKs.IsSubsetOf(relatedKeyMembers) && relatedKeyMembers.IsSubsetOf(relatedPKs);
            }
            return false;
        }

        public override bool IsRelationshipTarget(MappedEntity entity, MemberInfo member)
        {
            if (IsAssociationRelationship(entity, member))
            {
                if (typeof(IEnumerable).IsAssignableFrom(TypeHelper.GetMemberType(member)))
                    return true;

                // is target of relationship if the assoctions keys are the same as this entities primary key
                var pks = new HashSet<string>(this.GetPrimaryKeyMembers(entity).Select(m => m.Name));
                var keys = new HashSet<string>(this.GetAssociationKeyMembers(entity, member).Select(m => m.Name));
                return keys.IsSubsetOf(pks) && pks.IsSubsetOf(keys);
            }
            return false;
        }

        public override IReadOnlyList<MemberInfo> GetAssociationKeyMembers(MappedEntity entity, MemberInfo member)
        {
            List<MemberInfo> keyMembers;
            List<MemberInfo> relatedKeyMembers;
            this.GetAssociationKeys(entity, member, out keyMembers, out relatedKeyMembers);
            return keyMembers;
        }

        public override IReadOnlyList<MemberInfo> GetAssociationRelatedKeyMembers(MappedEntity entity, MemberInfo member)
        {
            List<MemberInfo> keyMembers;
            List<MemberInfo> relatedKeyMembers;
            this.GetAssociationKeys(entity, member, out keyMembers, out relatedKeyMembers);
            return relatedKeyMembers;
        }

        private void GetAssociationKeys(MappedEntity entity, MemberInfo member, out List<MemberInfo> keyMembers, out List<MemberInfo> relatedKeyMembers)
        {
            MappedEntity entity2 = GetRelatedEntity(entity, member);

            // find all members in common (same name)
            var map1 = this.GetMappedMembers(entity).Where(m => this.IsColumn(entity, m)).ToDictionary(m => m.Name);
            var map2 = this.GetMappedMembers(entity2).Where(m => this.IsColumn(entity2, m)).ToDictionary(m => m.Name);
            var commonNames = map1.Keys.Intersect(map2.Keys).OrderBy(k => k);
            keyMembers = new List<MemberInfo>();
            relatedKeyMembers = new List<MemberInfo>();
            foreach (string name in commonNames)
            {
                keyMembers.Add(map1[name]);
                relatedKeyMembers.Add(map2[name]);
            }
        }

        public override string GetTableName(MappedEntity entity)
        {
            return !string.IsNullOrEmpty(entity.EntityId) ? entity.EntityId : this.InferTableName(entity.StaticType);
        }

        private string InferTableName(Type rowType)
        {
            return SplitWords(Plural(rowType.Name));
        }

        public static string SplitWords(string name)
        {
            StringBuilder? sb = null;
            bool lastIsLower = char.IsLower(name[0]);

            for (int i = 0, n = name.Length; i < n; i++)
            {
                bool thisIsLower = char.IsLower(name[i]);
                if (lastIsLower && !thisIsLower)
                {
                    if (sb == null)
                    {
                        sb = new StringBuilder();
                        sb.Append(name, 0, i);
                    }
                    sb.Append(" ");
                }
                if (sb != null)
                {
                    sb.Append(name[i]);
                }
                lastIsLower = thisIsLower;
            }
            if (sb != null)
            {
                return sb.ToString();
            }
            return name;
        }

        public static string Plural(string name)
        {
            if (name.EndsWith("x", StringComparison.OrdinalIgnoreCase) 
                || name.EndsWith("ch", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith("ss", StringComparison.OrdinalIgnoreCase)) 
            {
                return name + "es";
            }
            else if (name.EndsWith("y", StringComparison.OrdinalIgnoreCase)) 
            {
                return name.Substring(0, name.Length - 1) + "ies";
            }
            else if (!name.EndsWith("s"))
            {
                return name + "s";
            }
            return name;
        }

        public static string Singular(string name)
        {
            if (name.EndsWith("es", StringComparison.OrdinalIgnoreCase))
            {
                string rest = name.Substring(0, name.Length - 2);
                if (rest.EndsWith("x", StringComparison.OrdinalIgnoreCase)
                    || name.EndsWith("ch", StringComparison.OrdinalIgnoreCase)
                    || name.EndsWith("ss", StringComparison.OrdinalIgnoreCase))
                {
                    return rest;
                }
            }
            if (name.EndsWith("ies", StringComparison.OrdinalIgnoreCase))
            {
                return name.Substring(0, name.Length - 3) + "y";
            }
            else if (name.EndsWith("s", StringComparison.OrdinalIgnoreCase)
                && !name.EndsWith("ss", StringComparison.OrdinalIgnoreCase))
            {
                return name.Substring(0, name.Length - 1);
            }
            return name;
        }
    }
#endif

}
