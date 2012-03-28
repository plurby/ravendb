using System;
using System.IO;
using Lucene.Net.Store;
using System.Linq;
using Microsoft.Isam.Esent.Interop;
using Directory = Lucene.Net.Store.Directory;

namespace Raven.Storage.Esent.Indexing
{
	public class EsentDirectory : Directory
	{
		private readonly IndexingStorage indexingStorage;
		private readonly string directory;

		public EsentDirectory(IndexingStorage indexingStorage, string directory)
		{
			this.indexingStorage = indexingStorage;
			this.directory = directory;

			base.SetLockFactory(new EsentLockFactory(indexingStorage, directory));
		}

		[Obsolete("For some Directory implementations (FSDirectory}, and its subclasses), this method silently filters its results to include only index files.  Please use ListAll instead, which does no filtering. ")]
		public override string[] List()
		{
			string[] results = null;
			indexingStorage.Batch(actions =>
			{
				results = actions.ListFilesInDirectory(directory).ToArray();
			});
			return results;
		}

		public override bool FileExists(string name)
		{
			bool exists = false;
			indexingStorage.Batch(actions =>
			{
				exists = actions.FileExistsInDirectory(directory, name);
			});
			return exists;
		}

		public override long FileModified(string name)
		{
			int version = -1;
			indexingStorage.Batch(actions =>
			{
				version = actions.GetVersionOfFileInDirectory(directory, name);
			});
			return version;
		}

		public override void TouchFile(string name)
		{
			indexingStorage.Batch(actions => actions.TouchFileInDirectory(directory, name));
		}

		public override void DeleteFile(string name)
		{
			indexingStorage.Batch(actions => actions.DeleteFileInDirectory(directory, name));
		}

		[Obsolete]
		public override void RenameFile(string from, string to)
		{
			indexingStorage.Batch(actions => actions.RenameFileInDirectory(directory, from, to));
		}

		public override long FileLength(string name)
		{
			long length = 0;
			indexingStorage.Batch(actions =>
			{
				length = actions.GetLengthOfFileInDirectory(directory, name);
			});
			return length;
		}

		public override IndexOutput CreateOutput(string name)
		{
			var batch = indexingStorage.GetCurrentBatch();
			var stream = batch.GetFileStream(directory, name, createIfMissing: true, write: true);
			return new StreamIndexOutput(stream);
		}

		public override IndexInput OpenInput(string name)
		{
			var batch = indexingStorage.GetCurrentBatch();
			var stream = batch.GetFileStream(directory, name, createIfMissing: false, write: false);
			return new StreamIndexInput(stream);
		}

		public override void Close()
		{
			Dispose();
		}

		public override void Dispose()
		{
			
		}

		public class StreamIndexInput : IndexInput
		{
			private readonly Stream stream;
			private bool isClone;

			public StreamIndexInput(Stream stream)
			{
				this.stream = stream;
			}

			public override byte ReadByte()
			{
				var readByte = stream.ReadByte();
				if(readByte == -1)
					throw new EndOfStreamException();
				return (byte)readByte;
			}

			public override void ReadBytes(byte[] b, int offset, int len)
			{
				int total = 0;
				do
				{
					int read = stream.Read(b, offset + total, len - total);
					if (read == 0)
						throw new EndOfStreamException();
					total += read;
				}
				while (total < len);
			}

			public override Object Clone()
			{
				var clone = (StreamIndexInput)base.Clone();
				clone.isClone = true;
				return clone;
			}
			
			public override void Close()
			{
				if (!isClone)
				  stream.Close();
			}

			public override long GetFilePointer()
			{
				return stream.Position;
			}

			public override void Seek(long pos)
			{
				stream.Seek(pos, SeekOrigin.Begin);
			}

			public override long Length()
			{
				return stream.Length;
			}
		}

		public class StreamIndexOutput : IndexOutput
		{
			private readonly Stream stream;

			public StreamIndexOutput(Stream stream)
			{
				this.stream = stream;
			}

			public override void WriteByte(byte b)
			{
				stream.WriteByte(b);
			}

			public override void WriteBytes(byte[] b, int offset, int length)
			{
				stream.Write(b, offset, length);
			}

			public override void Flush()
			{
				stream.Flush();
			}

			public override void Close()
			{
				stream.Dispose();
			}

			public override long GetFilePointer()
			{
				return stream.Position;
			}

			public override void Seek(long pos)
			{
				stream.Seek(pos, SeekOrigin.Begin);
			}

			public override long Length()
			{
				return stream.Length;
			}
		}

		public class EsentLockFactory : LockFactory
		{
			private readonly IndexingStorage indexingStorage;
			private readonly string directory;

			public EsentLockFactory(IndexingStorage indexingStorage, string directory)
			{
				this.indexingStorage = indexingStorage;
				this.directory = directory;
			}

			public override Lock MakeLock(string lockName)
			{
				return new EsentLock(indexingStorage.GetCurrentBatch(), directory, lockName);
			}

			public override void ClearLock(string lockName)
			{
				indexingStorage.GetCurrentBatch().DeleteFileInDirectory(directory, lockName);
			}
		}

		public class EsentLock : Lock
		{
			private readonly string lockName;
			private readonly IndexingStorageActionsAccessor batch;
			private readonly string directory;

			public EsentLock(IndexingStorageActionsAccessor batch, string directory, string lockName)
			{
				this.lockName = lockName;
				this.batch = batch;
				this.directory = directory;
			}

			public override bool Obtain()
			{
				return batch.TryCreateLock(directory, lockName);
			}

			public override void Release()
			{
				batch.ReleaseLock(directory, lockName);
			}

			public override bool IsLocked()
			{
				return batch.FileExistsInDirectory(directory, lockName);
			}
		}
	}
}