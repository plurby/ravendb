//-----------------------------------------------------------------------
// <copyright file="IndexingStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Extensions;

namespace Raven.Storage.Esent.Indexing
{
	public class IndexingStorage : CriticalFinalizerObject, IDisposable
	{
		private readonly ThreadLocal<IndexingStorageActionsAccessor> current = new ThreadLocal<IndexingStorageActionsAccessor>();
		private readonly string database;
		private readonly NameValueCollection settings;
		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
		private readonly string path;
		private bool disposed;

		private JET_INSTANCE instance;
		private readonly IndexingTablesColumnsCache indexingTablesColumnsCache = new IndexingTablesColumnsCache();

		static IndexingStorage()
		{
			try
			{
				SystemParameters.MaxInstances = 1024;
			}
			catch (EsentErrorException e)
			{
				if (e.Error != JET_err.AlreadyInitialized)
					throw;
			}
		}

		public IndexingStorage(string path, NameValueCollection settings)
		{
			this.settings = settings;
			this.path = path.ToFullPath();
			this.database = Path.Combine(this.path, "Indexes.ravendb");

			new StorageConfigurator(settings).LimitSystemCache();

			Api.JetCreateInstance(out instance, database + Guid.NewGuid());
		}

		public IndexingTablesColumnsCache IndexingTablesColumnsCache
		{
			get { return indexingTablesColumnsCache; }
		}

		public JET_INSTANCE Instance
		{
			get { return instance; }
		}

		public string Database
		{
			get { return database; }
		}

		public Guid Id { get; private set; }


		public void Dispose()
		{
			disposerLock.EnterWriteLock();
			try
			{
				if (disposed)
					return;
				GC.SuppressFinalize(this);
				Api.JetTerm2(instance, TermGrbit.Complete);
			}
			finally
			{
				disposed = true;
				disposerLock.ExitWriteLock();
			}
		}

		public bool Initialize()
		{
			try
			{
				new StorageConfigurator(settings).ConfigureInstance(instance, path);

				Api.JetInit(ref instance);

				var newDb = EnsureDatabaseIsCreatedAndAttachToDatabase();

				SetIdFromDb();

				indexingTablesColumnsCache.InitColumDictionaries(instance, database);

				return newDb;
			}
			catch (Exception e)
			{
				Dispose();
				throw new InvalidOperationException("Could not open transactional storage: " + database, e);
			}
		}

		private void SetIdFromDb()
		{
			try
			{
				instance.WithDatabase(database, (session, dbid) =>
				{
					using (var details = new Table(session, dbid, "details", OpenTableGrbit.ReadOnly))
					{
						Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
						var columnids = Api.GetColumnDictionary(session, details);
						var column = Api.RetrieveColumn(session, details, columnids["id"]);
						Id = new Guid(column);
						var schemaVersion = Api.RetrieveColumnAsString(session, details, columnids["schema_version"]);
						if (schemaVersion == IndexingSchemaCreator.SchemaVersion)
							return;
						throw new InvalidOperationException(string.Format("The version on disk ({0}) is different that the version supported by this library: {1}{2}You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.", schemaVersion, IndexingSchemaCreator.SchemaVersion, Environment.NewLine));
					}
				});
			}
			catch (Exception e)
			{
				throw new InvalidOperationException(
					"Could not read db details from disk. It is likely that there is a version difference between the library and the db on the disk." +
						Environment.NewLine +
							"You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.",
					e);
			}
		}

		private bool EnsureDatabaseIsCreatedAndAttachToDatabase()
		{
			using (var session = new Session(instance))
			{
				try
				{
					Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
					return false;
				}
				catch (EsentErrorException e)
				{
					if (e.Error == JET_err.DatabaseDirtyShutdown)
					{
						try
						{
							using (var recoverInstance = new Instance("Recovery instance for: " + database))
							{
								recoverInstance.Init();
								using (var recoverSession = new Session(recoverInstance))
								{
									new StorageConfigurator(settings).ConfigureInstance(recoverInstance.JetInstance, path);
									Api.JetAttachDatabase(recoverSession, database,
														  AttachDatabaseGrbit.DeleteCorruptIndexes);
									Api.JetDetachDatabase(recoverSession, database);
								}
							}
						}
						catch (Exception)
						{
						}

						Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
						return false;
					}
					if (e.Error != JET_err.FileNotFound)
						throw;
				}

				new IndexingSchemaCreator(session).Create(database);
				Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
				return true;
			}
		}

		~IndexingStorage()
		{
			try
			{
				Trace.WriteLine(
					"Disposing esent resources from finalizer! You should call Storage.Dispose() instead!");
				Api.JetTerm2(instance, TermGrbit.Abrupt);
			}
			catch (Exception exception)
			{
				try
				{
					Trace.WriteLine("Failed to dispose esent instance from finalizer because: " + exception);
				}
				catch
				{
				}
			}
		}

	    [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public void Batch(Action<IndexingStorageActionsAccessor> action)
		{
			if(Id == Guid.Empty)
				throw new InvalidOperationException("Cannot use Storage before Initialize was called");
			if (disposed)
			{
				Trace.WriteLine("Storage.Batch was called after it was disposed, call was ignored.");
				return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
			}
			if (current.Value != null)
			{
				action(current.Value);
				return;
			}
			disposerLock.EnterReadLock();
			try
			{
				ExecuteBatch(action);
			}
			finally
			{
				disposerLock.ExitReadLock();
				current.Value = null;
			}
		}

		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		private void ExecuteBatch(Action<IndexingStorageActionsAccessor> action)
		{
			if(current.Value != null)
			{
				action(current.Value);
				return;
			}

			try
			{
				using(var storageActionsAccessor = new IndexingStorageActionsAccessor(indexingTablesColumnsCache, instance, database))
				{
					current.Value = storageActionsAccessor;
					action(storageActionsAccessor);
					storageActionsAccessor.Commit();
				}
			}
			finally
			{
				current.Value = null;
			}
		}

		public IndexingStorageActionsAccessor GetCurrentBatch()
		{
			var storageActionsAccessor = current.Value;
			if(storageActionsAccessor == null)
				throw new InvalidOperationException("Not operating within a batch!");
			return storageActionsAccessor;
		}
	}
}
