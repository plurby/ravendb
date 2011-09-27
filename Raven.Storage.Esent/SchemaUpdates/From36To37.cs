using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Impl;
using Raven.Database.Indexing;

namespace Raven.Storage.Esent.SchemaUpdates
{
	public class From36To37 : ISchemaUpdate
	{
		#region ISchemaUpdate Members

		public string FromSchemaVersion
		{
			get { return "3.6"; }
		}

		public void Init(IUuidGenerator generator)
		{
		}

		public void Update(Session session, JET_DBID dbid)
		{
			Transaction tx;
			using (tx = new Transaction(session))
			{
				using (var table = new Table(session, dbid, "mapped_results",OpenTableGrbit.None))
				{
					Api.JetDeleteIndex(session, table, "by_reduce_key_and_view_hashed");

					var defaultVal = BitConverter.GetBytes(-1);
					JET_COLUMNID columnid;
					Api.JetAddColumn(session, table, "reduce_group_id", new JET_COLUMNDEF
					{
						coltyp = JET_coltyp.Long,
						grbit = ColumndefGrbit.ColumnNotNULL
					}, defaultVal, defaultVal.Length, out columnid);

					const string indexDef = "+reduce_key_and_view_hashed\0+reduce_group_id\0\0";
					Api.JetCreateIndex(session, table, "by_reduce_key_and_view_hashed", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
									   80);

					var tblKeyColumn = Api.GetColumnDictionary(session, table);

					Api.MoveBeforeFirst(session, table);
					while (Api.TryMoveNext(session, table))
					{
						using (var update = new Update(session, table, JET_prep.Insert))
						{
							var documentKey = Api.RetrieveColumnAsString(session, table, tblKeyColumn["document_key"], Encoding.Unicode);
							Api.SetColumn(session, table, tblKeyColumn["reduce_group_id"], MapReduceIndex.ComputeReduceGroupId(documentKey));
							update.Save();
						}
					}
				}

				tx.Commit(CommitTransactionGrbit.LazyFlush);
				tx.Dispose();
				tx = new Transaction(session);
			}

			using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
			{
				Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
				var columnids = Api.GetColumnDictionary(session, details);

				using (var update = new Update(session, details, JET_prep.Replace))
				{
					Api.SetColumn(session, details, columnids["schema_version"], "3.7", Encoding.Unicode);

					update.Save();
				}
			}
			tx.Commit(CommitTransactionGrbit.None);
			tx.Dispose();
		}


		#endregion
	}
}
