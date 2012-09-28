using System;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Bundles.Quotas.Size
{
	public class SizeQuotaConfiguration
	{
		private readonly DocumentDatabase database;
		private readonly int margin;
		private readonly long hardLimit, softLimit;
		private DateTime lastCheck;
		private VetoResult skipCheck = VetoResult.Allowed;
		private bool recheckOnDelete;

		public static SizeQuotaConfiguration GetConfiguration(DocumentDatabase database)
		{
			return
				(SizeQuotaConfiguration)
				database.ExtensionsState.GetOrAddAtomically("Raven.Bundles.Quotas.SizeQuotaConfiguration", s =>
				{
					var sizeQuotaConfiguration = new SizeQuotaConfiguration(database);
					return sizeQuotaConfiguration;
				});
		}

		public SizeQuotaConfiguration(DocumentDatabase database)
		{
			this.database = database;
			var hardLimitQuotaAsString = database.Configuration.Settings["Raven/Quotas/Size/HardLimitInKB"];
			var marginAsString = database.Configuration.Settings["Raven/Quotas/Size/SoftMarginInKB"];

			if (int.TryParse(marginAsString, out margin) == false)
				margin = 1024 * 1024;// 1 MB by default

			if (long.TryParse(hardLimitQuotaAsString, out hardLimit) == false)
			{
				softLimit = hardLimit = long.MaxValue;
			}
			else
			{
				softLimit = (hardLimit - margin)*1024; // KB -> Bytes
				hardLimit *= 1024; // KB -> Bytes
			}
		}

		public VetoResult AllowPut()
		{
			if (hardLimit == long.MaxValue)
				return VetoResult.Allowed;

			// checking the size of the database is pretty expensive, we only check it every so often, to reduce
			// its cost. This means that users might go beyond the limit, but that is okay, since the quota is soft
			// anyway
			if ((DateTime.UtcNow - lastCheck).TotalMinutes < 3)
				return skipCheck;

			UpdateSkippedCheck();

			return skipCheck;
		}

		private void UpdateSkippedCheck()
		{
			lastCheck = DateTime.UtcNow;

			var totalSizeOnDisk = database.GetTotalSizeOnDisk();
			if (totalSizeOnDisk <= softLimit)
			{
				database.Delete("Raven/Quotas/Size", null, null);
				skipCheck = VetoResult.Allowed;
				recheckOnDelete = false;
				return;
			}

			recheckOnDelete = true;

			string msg;
			if (totalSizeOnDisk > hardLimit) // beyond the grace margin
			{
				msg = string.Format("Database size is {0:#,#} KB, which is over the allowed quota of {1:#,#} KB. No more documents are allowed in.",
					totalSizeOnDisk / 1024, hardLimit / 1024);

				database.Put("Raven/Quotas/Size", null, new RavenJObject
				{
					{"Message", msg}
				}, new RavenJObject(), null);

				skipCheck = VetoResult.Deny(msg);
			}
			else // still before the hard limit, warn, but allow
			{
				msg = string.Format("Database size is {0:#,#} KB, which is close to the allowed quota of {1:#,#} KB",
					totalSizeOnDisk / 1024, softLimit / 1024);

				database.Put("Raven/Quotas/Size", null, new RavenJObject
				{
					{"Message", msg}
				}, new RavenJObject(), null);
				skipCheck = VetoResult.Allowed;
			}
		}

		public void AfterDelete()
		{
			if (recheckOnDelete == false)
				return;

			UpdateSkippedCheck();
		}
	}
}