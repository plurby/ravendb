// -----------------------------------------------------------------------
//  <copyright file="DelegatingStream.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent.Indexing
{
	public class DelegatingStream : Stream
	{
		private ColumnStream Stream
		{
			get
			{
				// note that we rely on the fact that we don't need to dispose ColumnStream
				var indexingStorageActionsAccessor = indexingStorage.GetCurrentBatch();
				return new ColumnStream(indexingStorageActionsAccessor.Session, indexingStorageActionsAccessor.Files, filesColumn)
				{
					Position = position
				};
			}
		}
		private readonly IndexingStorage indexingStorage;
		private readonly byte[] bookmark;
		private readonly bool write;
		private readonly Table files;
		private readonly JET_COLUMNID filesColumn;
		private long position;

		public DelegatingStream(IndexingStorage indexingStorage, byte[] bookmark, bool write, JET_COLUMNID filesColumn)
		{
			this.indexingStorage = indexingStorage;
			this.bookmark = bookmark;
			this.write = write;
			this.files = files;
			this.filesColumn = filesColumn;
		}

		private IndexingStorageActionsAccessor Accessor
		{
			get { return indexingStorage.GetCurrentBatch(); }
		}

		public override void Flush()
		{
			Accessor.GoToFileBookmark(bookmark);
			if (write == false)
				throw new IOException("Cannot write to this stream");
			using (var update = Accessor.BeginFileUpdate())
			{
				Stream.Flush();
				update.Save();
			}
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			return position = Stream.Seek(offset, origin);
		}

		public override void SetLength(long value)
		{
			Accessor.GoToFileBookmark(bookmark);
			if(write == false)
				throw new IOException("Cannot write to this stream");
			using(var update= Accessor.BeginFileUpdate())
			{
				Stream.SetLength(value);
				update.Save();
			}
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			Accessor.GoToFileBookmark(bookmark);
			return Stream.Read(buffer, offset, count);
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			Accessor.GoToFileBookmark(bookmark);
			if (write == false)
				throw new IOException("Cannot write to this stream");
			using (var update = Accessor.BeginFileUpdate())
			{
				Stream.Write(buffer, offset, count);
				update.Save();
			}
		}

		public override bool CanRead
		{
			get { return Stream.CanRead; }
		}

		public override bool CanSeek
		{
			get { return Stream.CanSeek; }
		}

		public override bool CanWrite
		{
			get { return Stream.CanWrite && write; }
		}

		public override long Length
		{
			get
			{
				Accessor.GoToFileBookmark(bookmark);
				return Stream.Length;
			}
		}
		public override long Position
		{
			get { return position; }
			set { position = Stream.Position = value; }
		}
	}
}