//-----------------------------------------------------------------------
// <copyright file="IndexingTablesColumnsCache.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent.Indexing
{
	public class IndexingTablesColumnsCache
	{

		public IDictionary<string, JET_COLUMNID> FilesColumns { get; set; }
		public IDictionary<string, JET_COLUMNID> DetailsColumns { get; set; }
		public IDictionary<string, JET_COLUMNID> LocksColumns { get; set; }

		public void InitColumDictionaries(JET_INSTANCE instance, string database)
		{
			using (var session = new Session(instance))
			{
				var dbid = JET_DBID.Nil;
				try
				{
					Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);
					using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
						DetailsColumns = Api.GetColumnDictionary(session, details);
					using (var locks = new Table(session, dbid, "locks", OpenTableGrbit.None))
						LocksColumns = Api.GetColumnDictionary(session, locks);
					using (var files = new Table(session, dbid, "files", OpenTableGrbit.None))
						FilesColumns = Api.GetColumnDictionary(session, files);
				}
				finally
				{
					if (Equals(dbid, JET_DBID.Nil) == false)
						Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
				}
			}
		}
	}
}
