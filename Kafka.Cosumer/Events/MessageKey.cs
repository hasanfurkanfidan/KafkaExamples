namespace Kafka.Cosumer.Events
{
    public record MessageKey
    {
        public string Key1 { get; set; } = default!;
        public string Key2 { get; set; } = default!;
    }
}
