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
		private Stream stream;
		private readonly IndexingStorageActionsAccessor accessor;
		private readonly byte[] bookmark;
		private bool write;

		public DelegatingStream(IndexingStorageActionsAccessor accessor, byte[] bookmark, bool write, Stream stream)
		{
			this.accessor = accessor;
			this.bookmark = bookmark;
			this.write = write;
			this.stream = stream;
		}


		public override void Flush()
		{
			accessor.GoToFileBookmark(bookmark);
			if (write == false)
				throw new IOException("Cannot write to this stream");
			using (var update = accessor.BeginFileUpdate())
			{
				stream.Flush();
				update.Save();
			}
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			return stream.Seek(offset, origin);
		}

		public override void SetLength(long value)
		{
			accessor.GoToFileBookmark(bookmark);
			if(write == false)
				throw new IOException("Cannot write to this stream");
			using(var update= accessor.BeginFileUpdate())
			{
				stream.SetLength(value);
				update.Save();
			}
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			accessor.GoToFileBookmark(bookmark);
			return stream.Read(buffer, offset, count);
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			accessor.GoToFileBookmark(bookmark);
			if (write == false)
				throw new IOException("Cannot write to this stream");
			using (var update = accessor.BeginFileUpdate())
			{
				stream.Write(buffer, offset, count);
				update.Save();
			}
		}

		public override bool CanRead
		{
			get { return stream.CanRead; }
		}

		public override bool CanSeek
		{
			get { return stream.CanSeek; }
		}

		public override bool CanWrite
		{
			get { return stream.CanWrite && write; }
		}

		public override long Length
		{
			get
			{
				accessor.GoToFileBookmark(bookmark);
				return stream.Length;
			}
		}
		public override long Position
		{
			get { return stream.Position; }
			set { stream.Position = value; }
		}
	}
}