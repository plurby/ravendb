//-----------------------------------------------------------------------
// <copyright file="IndexingStorageActionsAccessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent.Indexing
{
	public class IndexingStorageActionsAccessor : IDisposable
	{
		private readonly IndexingTablesColumnsCache indexingTablesColumnsCache;
		private readonly Session session;
		private readonly JET_DBID database;

		private Table files, locks, details;
		private readonly Transaction transaction;

		private Table Files
		{
			get { return files ?? (files = new Table(session, database, "files", OpenTableGrbit.None)); }
		}


		private Table Locks
		{
			get { return locks ?? (locks = new Table(session, database, "locks", OpenTableGrbit.None)); }
		}
		private Table Details
		{
			get { return details ?? (details = new Table(session, database, "details", OpenTableGrbit.None)); }
		}


		public IndexingStorageActionsAccessor(IndexingTablesColumnsCache indexingTablesColumnsCache, JET_INSTANCE instance, string databaseName)
		{
			this.indexingTablesColumnsCache = indexingTablesColumnsCache;
			try
			{
				session = new Session(instance);
				transaction = new Transaction(session);
				Api.JetOpenDatabase(session, databaseName, null, out database, OpenDatabaseGrbit.None);
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}
		}

		[DebuggerHidden]
		[DebuggerNonUserCode]
		public void Dispose()
		{

			if (details != null)
				details.Dispose();
			if (files != null)
				files.Dispose();
			if (locks != null)
				locks.Dispose();
			if (Equals(database, JET_DBID.Nil) == false)
				Api.JetCloseDatabase(session, database, CloseDatabaseGrbit.None);
			if (transaction != null)
				transaction.Dispose();
			if (session != null)
				session.Dispose();
		}

		[DebuggerHidden]
		[DebuggerNonUserCode]
		public void Commit()
		{
			transaction.Commit(CommitTransactionGrbit.None);
		}

		public IEnumerable<string> ListFilesInDirectory(string directory)
		{
			Api.JetSetCurrentIndex(session, Files, "by_path");
			Api.MakeKey(session, Files, directory, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
				yield break;
			Api.MakeKey(session, Files, directory, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, Files, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			do
			{
				yield return Api.RetrieveColumnAsString(session, Files, indexingTablesColumnsCache.FilesColumns["name"], Encoding.Unicode);
			} while (Api.TryMoveNext(session, Files));
		}

		public bool FileExistsInDirectory(string directory, string name)
		{
			Api.JetSetCurrentIndex(session, Files, "by_path");
			Api.MakeKey(session, Files, directory, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Files, name, Encoding.Unicode, MakeKeyGrbit.None);
			return Api.TrySeek(session, Files, SeekGrbit.SeekEQ);
		}

		public int GetVersionOfFileInDirectory(string directory, string name)
		{
			MoveToFile(directory, name);

			return Api.RetrieveColumnAsInt32(session, Files, indexingTablesColumnsCache.FilesColumns["modified"]) ?? 0;
		}

		private void MoveToFile(string directory, string name)
		{
			if (TryMoveTofile(directory, name) == false)
			{
				throw new FileNotFoundException("Could not find file in directory: " + directory, name);
			}
		}

		private bool TryMoveTofile(string directory, string name)
		{
			Api.JetSetCurrentIndex(session, Files, "by_path");
			Api.MakeKey(session, Files, directory, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Files, name, Encoding.Unicode, MakeKeyGrbit.None);
			return Api.TrySeek(session, Files, SeekGrbit.SeekEQ);
		}

		public void TouchFileInDirectory(string directory, string name)
		{
			MoveToFile(directory, name);
			Api.EscrowUpdate(session, Files, indexingTablesColumnsCache.FilesColumns["modified"], 1);
		}

		public void DeleteFileInDirectory(string directory, string name)
		{
			if (TryMoveTofile(directory, name))
				Api.JetDelete(session, Files);
		}

		public void RenameFileInDirectory(string directory, string src, string dest)
		{
			MoveToFile(directory, src);
			var oldVersion = (Api.RetrieveColumnAsInt32(session, Files, indexingTablesColumnsCache.FilesColumns["modified"]) ?? 0);
			using (var update = new Update(session, Files, JET_prep.Replace))
			{
				Api.SetColumn(session, Files, indexingTablesColumnsCache.FilesColumns["name"], dest, Encoding.Unicode);
				Api.SetColumn(session, Files, indexingTablesColumnsCache.FilesColumns["modified"], oldVersion + 1);

				update.Save();
			}
		}

		public long GetLengthOfFileInDirectory(string directory, string name)
		{
			MoveToFile(directory, name);
			return (Api.RetrieveColumnSize(session, Files, indexingTablesColumnsCache.FilesColumns["data"]) ?? 0);
		}

		public bool TryCreateLock(string directory, string name)
		{
			Api.JetSetCurrentIndex(session, Locks, "by_path");
			Api.MakeKey(session, Locks, directory, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Locks, name, Encoding.Unicode, MakeKeyGrbit.None);
			if (Api.TrySeek(session, Locks, SeekGrbit.SeekEQ))// already there
				return false;

			try
			{
				using(var update = new Update(session, Locks, JET_prep.Insert))
				{
					Api.SetColumn(session, Locks, indexingTablesColumnsCache.LocksColumns["directory"], directory, Encoding.Unicode);
					Api.SetColumn(session, Locks, indexingTablesColumnsCache.LocksColumns["name"], name, Encoding.Unicode);
					update.Save();
				}
				return true;
			}
			catch (EsentErrorException e)
			{
				if (e.Error == JET_err.KeyDuplicate)
					return false;
				throw;
			}

		}

		public Stream GetFileStream(string directory, string name, bool createIfMissing, bool write)
		{
			if (createIfMissing)
			{
				if (TryMoveTofile(directory, name) == false)
				{
					using (var update = new Update(session, Files, JET_prep.Insert))
					{
						Api.SetColumn(session, Files, indexingTablesColumnsCache.FilesColumns["name"], name, Encoding.Unicode);
						Api.SetColumn(session, Files, indexingTablesColumnsCache.FilesColumns["directory"], directory, Encoding.Unicode);
						Api.SetColumn(session, Files, indexingTablesColumnsCache.FilesColumns["data"], new byte[0]);

						update.SaveAndGotoBookmark();
					}
				}
			}
			else
			{
				MoveToFile(directory, name);
			}
			var bookmark = Api.GetBookmark(session, Files);
			var stream = new ColumnStream(session, Files, indexingTablesColumnsCache.FilesColumns["data"]);
			return new BufferedStream(new DelegatingStream(this, bookmark, write, stream));
		}

		public void ReleaseLock(string directory, string name)
		{
			Api.JetSetCurrentIndex(session, Locks, "by_path");
			Api.MakeKey(session, Locks, directory, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Locks, name, Encoding.Unicode, MakeKeyGrbit.None);
			if (Api.TrySeek(session, Locks, SeekGrbit.SeekEQ) == false)
				return;
			Api.JetDelete(session, Locks);

		}

		public void GoToFileBookmark(byte[] bookmark)
		{
			Api.JetGotoBookmark(session, Files, bookmark, bookmark.Length);
		}

		public Update BeginFileUpdate()
		{
			return new Update(session, Files, JET_prep.Replace);
		}
	}
}
