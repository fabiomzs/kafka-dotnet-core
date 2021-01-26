using System;

namespace FabioMuniz.Kafka.Sample.Domain
{
    public class Order
    {
        public Order(int quantity, decimal value, string company)
        {
            Id = Guid.NewGuid();
            Quantity = quantity;
            Value = value;
            Company = company;
        }

        public Guid Id { get; set; }
        public int Quantity { get; set; }
        public decimal Value { get; set; }
        public string Company { get; set; }
    }
}
