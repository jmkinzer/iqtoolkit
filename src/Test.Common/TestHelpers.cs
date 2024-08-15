using System;
using System.Collections.Generic;
using System.Linq;

using IQToolkit;
using IQToolkit.Entities;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Reflection;

namespace Test
{
    public static class TestHelpers
    {
        public static void TestQueryText<TValue>(
            IQueryable<TValue> query,
            params string[] expectedQueries)
        {
            var provider = (EntityProvider)query.Provider;
            var plan = provider.GetQueryPlan(query.Expression);
            if (plan.Diagnostics.Count > 0)
            {
                Assert.Fail($"Query had errors: {plan.Diagnostics[0].Message}");
            }

            var actualQueries = plan.QueryCommands.Select(c => c.CommandText).ToArray();

            Assert.AreEqual(expectedQueries.Length, actualQueries.Length, "number of database queries");
            for (int i = 0; i < expectedQueries.Length; i++)
            {
                Assert.AreEqual(Normalize(expectedQueries[i]), Normalize(actualQueries[i]));
            }
        }

        public static void TestQueryResults<TValue>(
            IQueryable<TValue> query,
            IEnumerable<TValue> expectedValues)
        {
            var provider = (EntityProvider)query.Provider;
            var actualValues = query.ToList();
            AssertAreEquivalent(expectedValues, actualValues);
        }

        private static char[] _lineEndings = new[] { '\r', '\n' };
        public static string Normalize(string text)
        {
            var parts = text.Split(_lineEndings, StringSplitOptions.RemoveEmptyEntries)
                .Select(text => text.Trim())
                .Where(text => !string.IsNullOrWhiteSpace(text))
                .ToArray();
            return string.Join("\n", parts);
        }

        public static void AssertAreEqualNormalized(string expected, string actual, string? message = null)
        {
            var expectedNormalized = Normalize(expected);
            var actualNormalized = Normalize(actual);
            Assert.AreEqual(expectedNormalized, actualNormalized, message);
        }

        public static void AssertAreEquivalent(object? expected, object? actual, string path = "")
        {
            if (expected == actual)
                return;

            if (expected == null && actual != null)
            {
                Assert.Fail($"{path}: expected: {expected} actual: {actual}");
            }
            else if (expected != null && actual == null)
            {
                Assert.Fail($"{path}: expected: {expected} actual: {actual}");
            }

            var expectedType = expected!.GetType();
            var actualType = actual!.GetType();

            if (expectedType != actualType)
            {
                Assert.Fail($"{path}: Type expected: {expectedType.Name} actual: {actualType.Name}");
            }

            if (typeof(IEquatable<>).MakeGenericType(expectedType).IsAssignableFrom(expectedType))
            {
                if (!object.Equals(expected, actual))
                {
                    Assert.Fail($"{path}: expected: {expected} actual: {actual}");
                }
            }
            else if (typeof(System.Collections.IEnumerable).IsAssignableFrom(expectedType))
            {
                var expectedList = ((System.Collections.IEnumerable)expected).OfType<object>().ToList();
                var actualList = ((System.Collections.IEnumerable)actual).OfType<object>().ToList();

                if (actualList.Count != expectedList.Count)
                {
                    Assert.Fail($"{path}: count expected: {expectedList.Count} actual: {actualList.Count}");
                }

                for (int i = 0; i < expectedList.Count; i++)
                {
                    var expectedItem = expectedList[i];
                    var actualItem = actualList[i];
                    AssertAreEquivalent(expectedItem, actualItem, $"{path}[{i}]");
                }
            }
            else
            {
                var props = expectedType.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy);
                foreach (var prop in props)
                {
                    if (prop.GetIndexParameters().Length == 0)
                    {
                        var expectedPropValue = prop.GetValue(expected);
                        var actualPropValue = prop.GetValue(actual);
                        AssertAreEquivalent(expectedPropValue, actualPropValue, $"{path}.{prop.Name}");
                    }
                }
            }
        }

        public static void AssertLargeTextEqual(string text1, string text2)
        {
            if (text1 != text2)
            {
                var lines1 = SplitLines(text1);
                var lines2 = SplitLines(text2);

                for (int i = 0; i < lines1.Length; i++)
                {
                    if (lines1[i] != lines2[i])
                    {
                        Assert.Fail($"Line {i + 1}:\nexpected: {lines1[i]}\nactual: {lines2[i]}");
                    }
                }
            }
        }

        public static string[] SplitLines(string text)
        {
            return text.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(line => line.Trim())
                .ToArray();
        }

        public static string[] SplitNames(string text)
        {
            return text.Split(new char[] { ',', ';', '|' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(t => t.Trim())
                .Where(t => !string.IsNullOrWhiteSpace(t))
                .ToArray();
        }
    }
}
