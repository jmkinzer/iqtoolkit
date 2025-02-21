﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;

#nullable disable
namespace Test
{
    using IQToolkit.Entities;
    using IQToolkit.Entities.Mapping;
    using IQToolkit.Entities.Sessions;

    public class Customer
    {
        public string CustomerID;
        public string ContactName;
        public string CompanyName;
        public string Phone;
        public string City;
        public string Country;
        public IList<Order> Orders = new List<Order>();
    }

    public class Order
    {
        public int OrderID;
        public string CustomerID;
        public DateTime OrderDate;
        public Customer Customer;
        public IList<OrderDetail> Details;
    }

    public class OrderDetail
    {
        public int? OrderID { get; set; }
        public int ProductID { get; set; }
        public Product Product;
        public Order Order;
    }

    public interface IEntity
    {
        int ID { get; }
    }

    public class Product : IEntity
    {
        public int ProductID;
        public string ProductName;
        public bool Discontinued;

        int IEntity.ID
        {
            get { return this.ProductID; }
        }
    }

    public class Employee
    {
        public int EmployeeID;
        public string LastName;
        public string FirstName;
        public string Title;
        public Address Address;
    }

    public class Address
    {
        public string Street { get; }
        public string City { get; }
        public string Region { get;  }
        public string PostalCode { get; }

        public Address(string street, string city, string region, string postalCode)
        {
            this.Street = street;
            this.City = city;
            this.Region = region;
            this.PostalCode = postalCode;
        }
    }

    public class Northwind
    {
        public IEntityProvider Provider { get; }

        public Northwind(IEntityProvider provider)
        {
            this.Provider = provider;
        }

        public virtual IEntityTable<Customer> Customers =>
            this.Provider.GetTable<Customer>(nameof(Customers));

        public virtual IEntityTable<Order> Orders =>
            this.Provider.GetTable<Order>(nameof(Orders));

        public virtual IEntityTable<OrderDetail> OrderDetails =>
            this.Provider.GetTable<OrderDetail>(nameof(OrderDetails));

        public virtual IEntityTable<Product> Products =>
            this.Provider.GetTable<Product>(nameof(Products));

        public virtual IEntityTable<Employee> Employees =>
            this.Provider.GetTable<Employee>(nameof(Employees));
    }

    public class NorthwindWithAttributes : Northwind
    {
        public NorthwindWithAttributes(IEntityProvider provider)
            : base(provider)
        {
        }
        [Entity(Id = "Customers")]
        [Table]
        [Column(Member = nameof(Customer.CustomerID), IsPrimaryKey = true)]
        [Column(Member = nameof(Customer.ContactName))]
        [Column(Member = nameof(Customer.CompanyName))]
        [Column(Member = nameof(Customer.Phone))]
        [Column(Member = nameof(Customer.City), DbType="NVARCHAR(20)")]
        [Column(Member = nameof(Customer.Country))]
        [Association(Member = nameof(Customer.Orders), KeyColumns = "CustomerID")]
        public override IEntityTable<Customer> Customers =>
            base.Customers;
        
        [Table]
        [Column(Member = nameof(Order.OrderID), IsPrimaryKey = true, IsGenerated = true)]
        [Column(Member = nameof(Order.CustomerID))]
        [Column(Member = nameof(Order.OrderDate))]
        [Association(Member = nameof(Order.Customer), KeyColumns="CustomerID", IsForeignKey=true)]
        [Association(Member = nameof(Order.Details), KeyColumns="OrderID")]
        public override IEntityTable<Order> Orders =>
            base.Orders;

        [Table(Name = "Order Details")]
        [Column(Member = nameof(OrderDetail.OrderID), IsPrimaryKey = true)]
        [Column(Member = nameof(OrderDetail.ProductID), IsPrimaryKey = true)]
        [Association(Member = nameof(OrderDetail.Product), KeyColumns="ProductID", RelatedEntityId="Products", IsForeignKey=true)]
        [Association(Member = nameof(OrderDetail.Order), KeyColumns="OrderID", IsForeignKey=true)]
        public override IEntityTable<OrderDetail> OrderDetails =>
            base.OrderDetails;

        [Table]
        [Column(Member = nameof(Product.ProductID), IsPrimaryKey = true)]
        [Column(Member = nameof(Product.ProductName))]
        [Column(Member = nameof(Product.Discontinued))]
        public override IEntityTable<Product> Products =>
            base.Products;

        [Table]
        [Column(Member = nameof(Employee.EmployeeID), IsPrimaryKey = true)]
        [Column(Member = nameof(Employee.LastName))]
        [Column(Member = nameof(Employee.FirstName))]
        [Column(Member = nameof(Employee.Title))]
        [Column(Member = "Address.Street", Name = "Address")]
        [Column(Member = "Address.City")]
        [Column(Member = "Address.Region")]
        [Column(Member = "Address.PostalCode")]
        public override IEntityTable<Employee> Employees =>
            base.Employees;
    }

    public interface INorthwindSession
    {
        void SubmitChanges();
        ISessionTable<Customer> Customers { get; }
        ISessionTable<Order> Orders { get; }
        ISessionTable<OrderDetail> OrderDetails { get; }
    }

    public class NorthwindSession : INorthwindSession
    {
        public IEntitySession Session { get; }

        public NorthwindSession(EntityProvider provider)
            : this(new EntitySession(provider))
        {
        }

        public NorthwindSession(IEntitySession session)
        {
            this.Session = session;
        }

        public void SubmitChanges()
        {
            this.Session.SubmitChanges();
        }

        public ISessionTable<Customer> Customers => 
            this.Session.GetTable<Customer>(nameof(Customers));

        public ISessionTable<Order> Orders =>
            this.Session.GetTable<Order>(nameof(Orders));

        public ISessionTable<OrderDetail> OrderDetails =>
            this.Session.GetTable<OrderDetail>(nameof(OrderDetails));
    }

    public class CustomerX
    {
        public CustomerX(string customerId, string contactName, string companyName, string phone, string city, string country, List<OrderX> orders)
        {
            this.CustomerID = customerId;
            this.ContactName = contactName;
            this.CompanyName = companyName;
            this.Phone = phone;
            this.City = city;
            this.Country = country;
            this.Orders = orders;
        }

        public string CustomerID { get; private set; }
        public string ContactName { get; private set; }
        public string CompanyName { get; private set; }
        public string Phone { get; private set; }
        public string City { get; private set; }
        public string Country { get; private set; }
        public List<OrderX> Orders { get; private set; }
    }

    public class OrderX
    {
        public OrderX(int orderID, string customerID, DateTime orderDate, CustomerX customer)
        {
            this.OrderID = orderID;
            this.CustomerID = customerID;
            this.OrderDate = orderDate;
            this.Customer = customer;
        }

        public int OrderID { get; private set; }
        public string CustomerID { get; private set; }
        public DateTime OrderDate { get; private set; }
        public CustomerX Customer { get; private set; }
    }

    public class NorthwindX
    {
        private readonly IEntityProvider _provider;

        public NorthwindX(IEntityProvider provider)
        {
            _provider = provider;
        }

        [Table]
        [Column(Member = nameof(CustomerX.CustomerID), IsPrimaryKey = true)]
        [Column(Member = nameof(CustomerX.ContactName))]
        [Column(Member = nameof(CustomerX.CompanyName))]
        [Column(Member = nameof(CustomerX.Phone))]
        [Column(Member = nameof(CustomerX.City), DbType = "NVARCHAR(20)")]
        [Column(Member = nameof(CustomerX.Country))]
        [Association(Member = nameof(CustomerX.Orders), KeyColumns = "CustomerID")]
        public IEntityTable<CustomerX> Customers =>
            _provider.GetTable<CustomerX>();

        [Table]
        [Column(Member = nameof(OrderX.OrderID), IsPrimaryKey = true, IsGenerated = true)]
        [Column(Member = nameof(OrderX.CustomerID))]
        [Column(Member = nameof(OrderX.OrderDate))]
        [Association(Member = nameof(OrderX.Customer), KeyColumns = "CustomerID")]
        public IEntityTable<OrderX> Orders =>
            _provider.GetTable<OrderX>();
    }
}
