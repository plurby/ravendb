using System;
using Lucene.Net.Index;
using NLog;

namespace Raven.Storage.Managed
{
	public class ErrorLoggingMergeScheduler : ConcurrentMergeScheduler
	{
		private readonly Logger log = LogManager.GetCurrentClassLogger();

		protected override void HandleMergeException(Exception exc)
		{
			try
			{
				base.HandleMergeException(exc);
			}
			catch (Exception e)
			{
				log.Warn("Concurrent merge failure", e);
			}
		}
	}
}