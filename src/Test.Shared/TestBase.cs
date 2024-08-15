using IQToolkit.Entities;
using IQToolkit.Entities.Mapping;
using IQToolkit.Entities.Sessions;
using IQToolkit;
using System.Data;
using System;

namespace Test
{
    public partial class TestBase
    {
        private static object _dbLock = new object();
        private static IDbConnection? _connection;

        protected void TestProvider(
            EntityMapping mapping,
            Action<IEntityProvider> fnTest)
        {
            // force serialization of integration tests
            lock (_dbLock)
            {
                if (_connection == null)
                {
                    _connection = TestProviders.CreateConnection();
                    // keep connection open because rapid open-close of connection slows down tests
                    _connection.Open();
                }

                var provider = TestProviders.CreateProvider(_connection, mapping);
                fnTest(provider);
            }
        }

        protected static readonly EntityMapping _defaultNorthwindMapping =
            new AttributeMapping(typeof(NorthwindWithAttributes));

        protected void TestNorthwind(
            Action<Northwind> fnTest,
            EntityMapping? mapping = null)
        {
            TestProvider(
                mapping ?? _defaultNorthwindMapping,
                provider => fnTest(new Northwind(provider))
                );
        }
    }
}