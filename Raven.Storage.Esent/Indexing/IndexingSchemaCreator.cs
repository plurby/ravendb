//-----------------------------------------------------------------------
// <copyright file="SchemaCreator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent.Indexing
{
	public class IndexingSchemaCreator
	{
		public const string SchemaVersion = "1.0";
		private readonly Session session;

		public IndexingSchemaCreator(Session session)
		{
			this.session = session;
		}

		public void Create(string database)
		{
			JET_DBID dbid;
			Api.JetCreateDatabase(session, database, null, out dbid, CreateDatabaseGrbit.None);
			try
			{
				using (var tx = new Transaction(session))
				{
					CreateDetailsTable(dbid);
					CreateFilesTable(dbid);
					CreateLocksTable(dbid);
					tx.Commit(CommitTransactionGrbit.None);
				}
			}
			finally
			{
				Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
			}
		}


		private void CreateDetailsTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "details", 1, 80, out tableid);
			JET_COLUMNID id;
			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				cbMax = 16,
				coltyp = JET_coltyp.Binary,
				grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
			}, null, 0, out id);

			JET_COLUMNID schemaVersion;
			Api.JetAddColumn(session, tableid, "schema_version", new JET_COLUMNDEF
			{
				cbMax = Encoding.Unicode.GetByteCount(SchemaVersion),
				cp = JET_CP.Unicode,
				coltyp = JET_coltyp.Text,
				grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
			}, null, 0, out schemaVersion);

			using (var update = new Update(session, tableid, JET_prep.Insert))
			{
				Api.SetColumn(session, tableid, id, Guid.NewGuid().ToByteArray());
				Api.SetColumn(session, tableid, schemaVersion, SchemaVersion, Encoding.Unicode);
				update.Save();
			}
		}

		private void CreateLocksTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "locks", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnAutoincrement|ColumndefGrbit.ColumnFixed |ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "directory", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "name", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);


			string indexDef = "+id\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   80);
	
			indexDef = "+directory\0+name\0\0";
			Api.JetCreateIndex(session, tableid, "by_path", CreateIndexGrbit.IndexUnique, indexDef, indexDef.Length,
							   80);
		}

		private void CreateFilesTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "files", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "directory", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "name", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			var defaultValue = BitConverter.GetBytes(1);
			Api.JetAddColumn(session, tableid, "modified", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnEscrowUpdate
			}, defaultValue, defaultValue.Length, out columnid);

			Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongBinary,
				cbMax = 1024 * 1024 * 1024,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);


			string indexDef = "+id\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   80);
			indexDef = "+directory\0+name\0\0";
			Api.JetCreateIndex(session, tableid, "by_path", CreateIndexGrbit.IndexUnique, indexDef, indexDef.Length,
							   80);
		}
	}
}
