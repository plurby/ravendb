using System;
using System.Runtime.CompilerServices;
using Lucene.Net.Index;
using NLog;

namespace Raven.Storage.Esent.Indexing
{
	public class EsentMergeConcurrentMergeScheduler : ConcurrentMergeScheduler
	{
		private readonly IndexingStorage indexingStorage;

		public EsentMergeConcurrentMergeScheduler(IndexingStorage indexingStorage)
		{
			this.indexingStorage = indexingStorage;
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		protected override MergeThread GetMergeThread(IndexWriter writer, MergePolicy.OneMerge merge)
		{
			MergeThread thread = new EsentMergeThread(this, writer, merge, indexingStorage);
			thread.SetThreadPriority(GetMergeThreadPriority());
			thread.IsBackground = true;
			thread.Name = "Lucene Merge Thread #" + mergeThreadCount++;
			return thread;
		}

		private static readonly Logger log = LogManager.GetCurrentClassLogger();

		protected override void HandleMergeException(Exception exc)
		{
			try
			{
				base.HandleMergeException(exc);
			}
			catch (Exception e)
			{
				log.WarnException("Concurrent merge failed", e);
			}
		}

		public class EsentMergeThread : MergeThread
		{
			private readonly IndexingStorage storage;

			public EsentMergeThread(EsentMergeConcurrentMergeScheduler esentMergeConcurrentMergeScheduler, IndexWriter indexWriter, MergePolicy.OneMerge merge, IndexingStorage storage)
				: base(esentMergeConcurrentMergeScheduler, indexWriter, merge)
			{
				this.storage = storage;
			}

			public override void Run()
			{
				storage.Batch(actions => BaseRun());
			}

			private void BaseRun()
			{
				base.Run();
			}
		}
	}
}