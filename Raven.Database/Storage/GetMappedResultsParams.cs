using Raven.Database.Indexing;

namespace Raven.Database.Storage
{
	public class GetMappedResultsParams
	{
		public GetMappedResultsParams(string view, ReduceKeyAndGroupId reduceKey, byte[] viewAndReduceKeyHashed)
		{
			View = view;
			ReduceKey = reduceKey;
			ViewAndReduceKeyHashed = viewAndReduceKeyHashed;
		}

		public string View { get; private set; }

		public ReduceKeyAndGroupId ReduceKey { get; private set; }

		public byte[] ViewAndReduceKeyHashed { get; private set; }
	}
}