﻿using System;
using System.Data;
using IQToolkit.Entities;

namespace Test
{
    public static class TestProviders
    {
        public static IDbConnection CreateConnection()
        {
            throw new NotImplementedException();
        }

        public static EntityProvider CreateProvider(IDbConnection connection, EntityMapping mapping)
        {
            throw new NotImplementedException();
        }
    }
}