//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET_3_5

using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;

namespace Raven.Client
{
	/// <summary>
	/// Advanced async session operations
	/// </summary>
	public interface IAsyncAdvancedSessionOperations : IAdvancedDocumentSessionOperations
	{
		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		IAsyncDatabaseCommands AsyncDatabaseCommands { get; }

		/// <summary>
		/// Load documents with the specified key prefix
		/// </summary>
		Task<IEnumerable<T>> LoadStartingWithAsync<T>(string keyPrefix, int start = 0, int pageSize = 25);


		/// <summary>
		/// Query the specified index using Lucene syntax
		/// </summary>
		IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string index);

		/// <summary>
		/// Dynamically query RavenDB using Lucene syntax
		/// </summary>
		IAsyncDocumentQuery<T> AsyncLuceneQuery<T>();
	}
}
#endif