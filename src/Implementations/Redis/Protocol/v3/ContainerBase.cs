using System.Collections.Generic;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public abstract class ContainerBase : AttributedDataType
    {
#nullable disable // Will be constructed in implementing classes
        public ICollection<DataType> Elements { get; protected set; }
#nullable enable  // Will be constructed in implementing classes

        internal void Add(DataType dt)
        {
            Elements.Add(dt);
        }

        internal void AddRange(IEnumerable<DataType> dts)
        {
            foreach (DataType dt in dts)
            {
                Elements.Add(dt);
            }
        }
    }
}
