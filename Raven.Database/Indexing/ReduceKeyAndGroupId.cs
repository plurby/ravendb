using System;

namespace Raven.Database.Indexing
{
	public class ReduceKeyAndGroupId
	{
		public string ReduceKey { get; set; }
		public int ReduceGroupId { get; set; }

		public bool Equals(ReduceKeyAndGroupId other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return string.Equals(other.ReduceKey, ReduceKey,StringComparison.InvariantCultureIgnoreCase) && other.ReduceGroupId == ReduceGroupId;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof (ReduceKeyAndGroupId)) return false;
			return Equals((ReduceKeyAndGroupId) obj);
		}

		public override string ToString()
		{
			return string.Format("[ReduceKey: {0}, ReduceGroupId: {1}]", ReduceKey, ReduceGroupId);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return ((ReduceKey != null ? ReduceKey.GetHashCode() : 0)*397) ^ ReduceGroupId;
			}
		}
	}
}