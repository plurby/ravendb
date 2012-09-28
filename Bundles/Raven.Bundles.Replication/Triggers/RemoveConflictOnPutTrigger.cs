//-----------------------------------------------------------------------
// <copyright file="RemoveConflictOnPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Order", 10000)]
	public class RemoveConflictOnPutTrigger : AbstractPutTrigger
	{
		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			using (Database.DisableAllTriggersForCurrentThread())
			{
				metadata.Remove(ReplicationConstants.RavenReplicationConflict);// you can't put conflicts

				var oldVersion = Database.Get(key, transactionInformation);
				if (oldVersion == null)
					return;
				if (oldVersion.Metadata[ReplicationConstants.RavenReplicationConflict] == null)
					return;

				RavenJArray history = metadata.Value<RavenJArray>(ReplicationConstants.RavenReplicationHistory) ?? new RavenJArray();
				metadata[ReplicationConstants.RavenReplicationHistory] = history;

				var ravenJTokenEqualityComparer = new RavenJTokenEqualityComparer();
				// this is a conflict document, holding document keys in the 
				// values of the properties
				foreach (var prop in oldVersion.DataAsJson.Value<RavenJArray>("Conflicts"))
				{
					RavenJObject deletedMetadata;
					Database.Delete(prop.Value<string>(), null, transactionInformation, out deletedMetadata);

					// add the conflict history to the mix, so we make sure that we mark that we resolved the conflict
					var conflictHistory = deletedMetadata.Value<RavenJArray>(ReplicationConstants.RavenReplicationHistory) ?? new RavenJArray();
					conflictHistory.Add(new RavenJObject
					{
						{ReplicationConstants.RavenReplicationVersion, deletedMetadata[ReplicationConstants.RavenReplicationVersion]},
						{ReplicationConstants.RavenReplicationSource, deletedMetadata[ReplicationConstants.RavenReplicationSource]}
					});

					foreach (var item in conflictHistory)
					{
						if(history.Any(x=>ravenJTokenEqualityComparer.Equals(x, item)))
							continue;
						history.Add(item);
					}
				}
			}
		}
	}
}
