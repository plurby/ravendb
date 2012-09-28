//-----------------------------------------------------------------------
// <copyright file="DocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
#if !NET_3_5
using Raven.Client.Connection.Async;
using System.Threading.Tasks;
using Raven.Client.Document.Batches;
#endif
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Exceptions;
using Raven.Client.Linq;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;

namespace Raven.Client.Document
{
	/// <summary>
	///   A query against a Raven index
	/// </summary>
	public abstract class AbstractDocumentQuery<T, TSelf> : IDocumentQueryCustomization, IRavenQueryInspector, IAbstractDocumentQuery<T>
	{
		protected bool isSpatialQuery;
		protected double lat, lng, radius;

		/// <summary>
		/// Whatever to negate the next operation
		/// </summary>
		protected bool negate;
#if !SILVERLIGHT
		/// <summary>
		/// The database commands to use
		/// </summary>
		protected readonly IDatabaseCommands theDatabaseCommands;
#endif
#if !NET_3_5
		/// <summary>
		/// Async database commands to use
		/// </summary>
		protected readonly IAsyncDatabaseCommands theAsyncDatabaseCommands;
#endif
		/// <summary>
		/// The index to query
		/// </summary>
		protected readonly string indexName;

		protected Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> transformResultsFunc;

		protected string defaultField;

		private int currentClauseDepth;

		private KeyValuePair<string, string> lastEquality;

		/// <summary>
		///   The list of fields to project directly from the index
		/// </summary>
		protected readonly string[] projectionFields;

		/// <summary>
		/// The query listeners for this query
		/// </summary>
		protected readonly IDocumentQueryListener[] queryListeners;
		/// <summary>
		/// The session for this query
		/// </summary>
		protected readonly InMemoryDocumentSessionOperations theSession;

		/// <summary>
		///   The cutoff date to use for detecting staleness in the index
		/// </summary>
		protected DateTime? cutoff;

		/// <summary>
		///   The fields to order the results by
		/// </summary>
		protected string[] orderByFields = new string[0];


		/// <summary>
		///   The types to sort the fields by (NULL if not specified)
		/// </summary>
		protected HashSet<KeyValuePair<string, Type>> sortByHints = new HashSet<KeyValuePair<string, Type>>();

		/// <summary>
		///   The page size to use when querying the index
		/// </summary>
		protected int? pageSize;

		protected QueryOperation queryOperation;

		/// <summary>
		/// The query to use
		/// </summary>
		protected StringBuilder theQueryText = new StringBuilder();

		/// <summary>
		///   which record to start reading from
		/// </summary>
		protected int start;

		/// <summary>
		/// Timeout for this query
		/// </summary>
		protected TimeSpan timeout;
		/// <summary>
		/// Should we wait for non stale results
		/// </summary>
		protected bool theWaitForNonStaleResults;
		/// <summary>
		/// The paths to include when loading the query
		/// </summary>
		protected HashSet<string> includes = new HashSet<string>();
		/// <summary>
		/// What aggregated operation to execute
		/// </summary>
		protected AggregationOperation aggregationOp;
		/// <summary>
		/// Fields to group on
		/// </summary>
		protected string[] groupByFields;

		/// <summary>
		/// Holds the query stats
		/// </summary>
		protected RavenQueryStatistics queryStats = new RavenQueryStatistics();

		/// <summary>
		///   Get the name of the index being queried
		/// </summary>
		public string IndexQueried
		{
			get { return indexName; }
		}

#if !SILVERLIGHT
		/// <summary>
		///   Grant access to the database commands
		/// </summary>
		public virtual IDatabaseCommands DatabaseCommands
		{
			get { return theDatabaseCommands; }
		}
#endif

#if !NET_3_5
		/// <summary>
		///   Grant access to the async database commands
		/// </summary>
		public virtual IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { return theAsyncDatabaseCommands; }
		}
#endif

		/// <summary>
		/// Gets the document convention from the query session
		/// </summary>
		public DocumentConvention DocumentConvention
		{
			get { return theSession.Conventions; }
		}

#if !SILVERLIGHT
		/// <summary>
		///   Gets the session associated with this document query
		/// </summary>
		public IDocumentSession Session
		{
			get { return (IDocumentSession)theSession; }
		}
#endif

		InMemoryDocumentSessionOperations IRavenQueryInspector.Session
		{
			get { return theSession; }
		}

		/// <summary>
		///   Gets the query text built so far
		/// </summary>
		protected StringBuilder QueryText
		{
			get { return theQueryText; }
		}

		protected Action<QueryResult> afterQueryExecutedCallback;
		private Guid? cutoffEtag;

		private TimeSpan DefaultTimeout
		{
			get
			{
				if (Debugger.IsAttached) // increase timeout if we are debugging
					return TimeSpan.FromMinutes(15);
				return TimeSpan.FromSeconds(15);
			}
		}

#if !SILVERLIGHT && !NET_3_5
		/// <summary>
		///   Initializes a new instance of the <see cref = "DocumentQuery{T}" /> class.
		/// </summary>
		protected AbstractDocumentQuery(InMemoryDocumentSessionOperations theSession,
									 IDatabaseCommands databaseCommands,
									 string indexName,
									 string[] projectionFields,
									 IDocumentQueryListener[] queryListeners)
			: this(theSession, databaseCommands, null, indexName, projectionFields, queryListeners)
		{
		}
#endif

		/// <summary>
		/// Initializes a new instance of the <see cref="AbstractDocumentQuery{T, TSelf}"/> class.
		/// </summary>
		public AbstractDocumentQuery(InMemoryDocumentSessionOperations theSession,
#if !SILVERLIGHT
									 IDatabaseCommands databaseCommands,
#endif
#if !NET_3_5
									 IAsyncDatabaseCommands asyncDatabaseCommands,
#endif
									 string indexName,
									 string[] projectionFields,
									 IDocumentQueryListener[] queryListeners)
		{
#if !SILVERLIGHT
			this.theDatabaseCommands = databaseCommands;
#endif
			this.projectionFields = projectionFields;
			this.queryListeners = queryListeners;
			this.indexName = indexName;
			this.theSession = theSession;
#if !NET_3_5
			this.theAsyncDatabaseCommands = asyncDatabaseCommands;
#endif
			this.AfterQueryExecuted(queryStats.UpdateQueryStats);

			if (this.theSession != null &&  // tests may decide to send null here
				this.theSession.DocumentStore.Conventions.DefaultQueryingConsistency == ConsistencyOptions.QueryYourWrites)
				WaitForNonStaleResultsAsOfLastWrite();
		}

		/// <summary>
		///   Initializes a new instance of the <see cref = "IDocumentQuery{T}" /> class.
		/// </summary>
		/// <param name = "other">The other.</param>
		protected AbstractDocumentQuery(AbstractDocumentQuery<T, TSelf> other)
		{
#if !SILVERLIGHT
			theDatabaseCommands = other.theDatabaseCommands;
#endif
#if !NET_3_5
			theAsyncDatabaseCommands = other.theAsyncDatabaseCommands;
#endif
			indexName = other.indexName;
			projectionFields = other.projectionFields;
			theSession = other.theSession;
			cutoff = other.cutoff;
			orderByFields = other.orderByFields;
			sortByHints = other.sortByHints;
			pageSize = other.pageSize;
			theQueryText = other.theQueryText;
			start = other.start;
			timeout = other.timeout;
			theWaitForNonStaleResults = other.theWaitForNonStaleResults;
			includes = other.includes;
			queryListeners = other.queryListeners;
			queryStats = other.queryStats;

			AfterQueryExecuted(queryStats.UpdateQueryStats);
		}

		#region TSelf Members

		/// <summary>
		///   Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name = "path">The path.</param>
		IDocumentQueryCustomization IDocumentQueryCustomization.Include(string path)
		{
			Include(path);
			return this;
		}

		/// <summary>
		///   EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
		///   This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		/// <param name = "waitTimeout">The wait timeout.</param>
		IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResults(TimeSpan waitTimeout)
		{
			WaitForNonStaleResults(waitTimeout);
			return this;
		}
		
		/// <summary>
		///   Filter matches to be inside the specified radius
		/// </summary>
		/// <param name = "radius">The radius.</param>
		/// <param name = "latitude">The latitude.</param>
		/// <param name = "longitude">The longitude.</param>
		IDocumentQueryCustomization IDocumentQueryCustomization.WithinRadiusOf(double radius, double latitude,
																			   double longitude)
		{
			GenerateQueryWithinRadiusOf(radius, latitude, longitude);
			return this;
		}

		/// <summary>
		///   Filter matches to be inside the specified radius
		/// </summary>
		/// <param name = "radius">The radius.</param>
		/// <param name = "latitude">The latitude.</param>
		/// <param name = "longitude">The longitude.</param>
		protected abstract object GenerateQueryWithinRadiusOf(double radius, double latitude, double longitude);

		/// <summary>
		///   EXPERT ONLY: Instructs the query to wait for non stale results.
		///   This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResults()
		{
			WaitForNonStaleResults();
			return this;
		}

		public void UsingDefaultField(string field)
		{
			defaultField = field;
		}

		/// <summary>
		///   Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name = "path">The path.</param>
		IDocumentQueryCustomization IDocumentQueryCustomization.Include<TResult>(Expression<Func<TResult, object>> path)
		{
			var body = path.Body as UnaryExpression;
			if (body != null)
			{
				switch (body.NodeType)
				{
					case ExpressionType.Convert:
					case ExpressionType.ConvertChecked:
						throw new InvalidOperationException("You cannot use Include<TResult> on value type. Please use the Include<TResult, TInclude> overload.");
				}
			}
			
			Include(path.ToPropertyPath());
			return this;
		}

		public IDocumentQueryCustomization Include<TResult, TInclude>(Expression<Func<TResult, object>> path)
		{
			var fullId = DocumentConvention.FindFullDocumentKeyFromNonStringIdentifier(-1, typeof (TInclude), false);
			var idPrefix = fullId.Replace("-1", string.Empty);

			var id = path.ToPropertyPath() + "(" + idPrefix + ")";
			Include(id);
			return this;
		}

		/// <summary>
		///   Instruct the query to wait for non stale result for the specified wait timeout.
		/// </summary>
		/// <param name = "waitTimeout">The wait timeout.</param>
		/// <returns></returns>
		public void WaitForNonStaleResults(TimeSpan waitTimeout)
		{
			theWaitForNonStaleResults = true;
		    cutoffEtag = null;
		    cutoff = null;
			timeout = waitTimeout;
		}

		protected QueryOperation InitializeQueryOperation(Action<string, string> setOperationHeaders)
		{
			var query = theQueryText.ToString();
			var indexQuery = GenerateIndexQuery(query);
			return new QueryOperation(theSession,
												indexName,
												indexQuery,
												projectionFields,
												sortByHints,
												theWaitForNonStaleResults,
												setOperationHeaders,
												timeout,
												transformResultsFunc);
		}

#if !SILVERLIGHT
		/// <summary>
		///   Gets the query result
		///   Execute the query the first time that this is called.
		/// </summary>
		/// <value>The query result.</value>
		public QueryResult QueryResult
		{
			get
			{
				InitSync();

				return queryOperation.CurrentQueryResults.CreateSnapshot();
			}
		}

		protected virtual void InitSync()
		{
			if (queryOperation != null) 
				return;
			theSession.IncrementRequestCount();
			ClearSortHints(DatabaseCommands);
			ExecuteBeforeQueryListeners();
			queryOperation = InitializeQueryOperation(DatabaseCommands.OperationsHeaders.Set);
			ExecuteActualQuery();
		}

		protected void ClearSortHints(IDatabaseCommands shardDbCommands)
		{
			foreach (var key in shardDbCommands.OperationsHeaders.AllKeys.Where(key => key.StartsWith("SortHint")).ToArray())
			{
				shardDbCommands.OperationsHeaders.Remove(key);
			}
		}


		protected virtual void ExecuteActualQuery()
		{
			while (true)
			{
				using (queryOperation.EnterQueryContext())
				{
					queryOperation.LogQuery();
					var result = DatabaseCommands.Query(indexName, queryOperation.IndexQuery, includes.ToArray());
					if (queryOperation.IsAcceptable(result) == false)
					{
						Thread.Sleep(100);
						continue;
					}
					break;
				}
			}
			InvokeAfterQueryExecuted(queryOperation.CurrentQueryResults);
		}
#endif

#if !NET_3_5 && !SILVERLIGHT

		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed
		/// </summary>
		public Lazy<IEnumerable<T>> Lazily()
		{
			return Lazily(null);
		}

		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed
		/// </summary>
		public virtual Lazy<IEnumerable<T>> Lazily(Action<IEnumerable<T>> onEval)
		{
			if (queryOperation == null)
			{
				foreach (var key in DatabaseCommands.OperationsHeaders.AllKeys.Where(key => key.StartsWith("SortHint")).ToArray())
				{
					DatabaseCommands.OperationsHeaders.Remove(key);
				}
				ExecuteBeforeQueryListeners();
				queryOperation = InitializeQueryOperation(DatabaseCommands.OperationsHeaders.Set);
			}

			var lazyQueryOperation = new LazyQueryOperation<T>(queryOperation, afterQueryExecutedCallback, includes);

			return ((DocumentSession)theSession).AddLazyOperation(lazyQueryOperation, onEval);
		}

#endif

#if !NET_3_5

		/// <summary>
		///   Gets the query result
		///   Execute the query the first time that this is called.
		/// </summary>
		/// <value>The query result.</value>
		public Task<QueryResult> QueryResultAsync
		{
			get
			{
				return InitAsync()
					.ContinueWith(x => x.Result.CurrentQueryResults.CreateSnapshot());
			}
		}

		private Task<QueryOperation> InitAsync()
		{
			if (queryOperation != null)
				return TaskResult(queryOperation);
			foreach (var key in AsyncDatabaseCommands.OperationsHeaders.Keys.Where(key => key.StartsWith("SortHint")).ToArray())
			{
				AsyncDatabaseCommands.OperationsHeaders.Remove(key);
			}
			ExecuteBeforeQueryListeners();

			queryOperation = InitializeQueryOperation((key, val) => AsyncDatabaseCommands.OperationsHeaders[key] = val);
			theSession.IncrementRequestCount();
			return ExecuteActualQueryAsync();
		}
#endif

		protected void ExecuteBeforeQueryListeners()
		{
			foreach (var documentQueryListener in queryListeners)
			{
				documentQueryListener.BeforeQueryExecuted(this);
			}
		}

		/// <summary>
		///   Gets the fields for projection
		/// </summary>
		/// <returns></returns>
		public IEnumerable<string> GetProjectionFields()
		{
			return projectionFields ?? Enumerable.Empty<string>();
		}

		/// <summary>
		/// Order the search results randomly
		/// </summary>
		public void RandomOrdering()
		{
			AddOrder(Constants.RandomFieldName + ";" + Guid.NewGuid(), false);
		}

		/// <summary>
		/// Order the search results randomly using the specified seed
		/// this is useful if you want to have repeatable random queries
		/// </summary>
		public void RandomOrdering(string seed)
		{
			AddOrder(Constants.RandomFieldName + ";" + seed, false);
		}

		public IDocumentQueryCustomization TransformResults(Func<IndexQuery,IEnumerable<object>, IEnumerable<object>> resultsTransformer)
		{
			this.transformResultsFunc = resultsTransformer;
			return this;
		}

		/// <summary>
		///   Adds an ordering for a specific field to the query
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "descending">if set to <c>true</c> [descending].</param>
		public void AddOrder(string fieldName, bool descending)
		{
			AddOrder(fieldName, descending, null);
		}

		/// <summary>
		///   Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "descending">if set to <c>true</c> [descending].</param>
		/// <param name = "fieldType">the type of the field to be sorted.</param>
		public void AddOrder(string fieldName, bool descending, Type fieldType)
		{
			fieldName = EnsureValidFieldName(new WhereParams
			{
				FieldName = fieldName
			});
			fieldName = descending ? "-" + fieldName : fieldName;
			orderByFields = orderByFields.Concat(new[] { fieldName }).ToArray();
			sortByHints.Add(new KeyValuePair<string, Type>(fieldName, fieldType));
		}

#if !SILVERLIGHT
		/// <summary>
		///   Gets the enumerator.
		/// </summary>
		public virtual IEnumerator<T> GetEnumerator()
		{
			InitSync();
			while (true)
			{
				try
				{
					return queryOperation.Complete<T>().GetEnumerator();
				}
				catch (Exception e)
				{
					if (queryOperation.ShouldQueryAgain(e) == false)
						throw;
					ExecuteActualQuery(); // retry the query, note that we explicitly not incrementing the session request count here
				}
			}
		}
#endif

#if !NET_3_5
	 
		private Task<Tuple<QueryOperation,IList<T>>> ProcessEnumerator(Task<QueryOperation> task)
		{
			var currentQueryOperation = task.Result;
			try
			{
				var list = currentQueryOperation.Complete<T>();
				return Task.Factory.StartNew(() => Tuple.Create(currentQueryOperation, list));
			}
			catch (Exception e)
			{
				if (queryOperation.ShouldQueryAgain(e) == false)
					throw;
				return ExecuteActualQueryAsync()
					.ContinueWith(t => ProcessEnumerator(t))
					.Unwrap();
			}
		}

#endif
		/// <summary>
		///   Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name = "path">The path.</param>
		public void Include(string path)
		{
			includes.Add(path);
		}

		/// <summary>
		///   This function exists solely to forbid in memory where clause on IDocumentQuery, because
		///   that is nearly always a mistake.
		/// </summary>
		[Obsolete(
			@"
You cannot issue an in memory filter - such as Where(x=>x.Name == ""Ayende"") - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.LuceneQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.LuceneQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.LuceneQuery<T>().ToList().Where(x=>x.Name == ""Ayende"")
"
			, true)]
		public IEnumerable<T> Where(Func<T, bool> predicate)
		{
			throw new NotSupportedException();
		}


		/// <summary>
		///   This function exists solely to forbid in memory where clause on IDocumentQuery, because
		///   that is nearly always a mistake.
		/// </summary>
		[Obsolete(
			@"
You cannot issue an in memory filter - such as Count(x=>x.Name == ""Ayende"") - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.LuceneQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.LuceneQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.LuceneQuery<T>().ToList().Count(x=>x.Name == ""Ayende"")
"
			, true)]
		public int Count(Func<T, bool> predicate)
		{
			throw new NotSupportedException();
		}

		/// <summary>
		///   This function exists solely to forbid in memory where clause on IDocumentQuery, because
		///   that is nearly always a mistake.
		/// </summary>
		[Obsolete(
			@"
You cannot issue an in memory filter - such as Count() - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.LuceneQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.LuceneQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.LuceneQuery<T>().ToList().Count()
"
			, true)]
		public int Count()
		{
			throw new NotSupportedException();
		}

		/// <summary>
		///   Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name = "path">The path.</param>
		public void Include(Expression<Func<T, object>> path)
		{
			Include(path.ToPropertyPath());
		}

		/// <summary>
		///   Takes the specified count.
		/// </summary>
		/// <param name = "count">The count.</param>
		/// <returns></returns>
		public void Take(int count)
		{
			pageSize = count;
		}

		/// <summary>
		///   Skips the specified count.
		/// </summary>
		/// <param name = "count">The count.</param>
		/// <returns></returns>
		public void Skip(int count)
		{
			start = count;
		}

		/// <summary>
		///   Filter the results from the index using the specified where clause.
		/// </summary>
		/// <param name = "whereClause">The where clause.</param>
		public void Where(string whereClause)
		{
			if (theQueryText.Length > 0)
			{
				theQueryText.Append(" ");
			}

			theQueryText.Append(whereClause);
		}

		/// <summary>
		///   Matches exact value
		/// </summary>
		/// <remarks>
		///   Defaults to NotAnalyzed
		/// </remarks>
		public void WhereEquals(string fieldName, object value)
		{
			WhereEquals(new WhereParams
			{
				FieldName = fieldName,
				Value = value
			});
		}

		/// <summary>
		///   Matches exact value
		/// </summary>
		/// <remarks>
		///   Defaults to allow wildcards only if analyzed
		/// </remarks>
		public void WhereEquals(string fieldName, object value, bool isAnalyzed)
		{
			WhereEquals(new WhereParams
			{
				AllowWildcards = isAnalyzed,
				IsAnalyzed = isAnalyzed,
				FieldName = fieldName,
				Value = value
			});
		}


		/// <summary>
		///   Simplified method for opening a new clause within the query
		/// </summary>
		/// <returns></returns>
		public void OpenSubclause()
		{
			currentClauseDepth++;
			if (theQueryText.Length > 0 && theQueryText[theQueryText.Length - 1] != '(')
			{
				theQueryText.Append(" ");
			}
			NegateIfNeeded();
			theQueryText.Append("(");
		}

		///<summary>
		///  Instruct the index to group by the specified fields using the specified aggregation operation
		///</summary>
		///<remarks>
		///  This is only valid on dynamic indexes queries
		///</remarks>
		public void GroupBy(AggregationOperation aggregationOperation, params string[] fieldsToGroupBy)
		{
			groupByFields = fieldsToGroupBy;
			aggregationOp = aggregationOperation;
		}

		/// <summary>
		///   Simplified method for closing a clause within the query
		/// </summary>
		/// <returns></returns>
		public void CloseSubclause()
		{
			currentClauseDepth--;
			theQueryText.Append(")");
		}

		/// <summary>
		///   Matches exact value
		/// </summary>
		public void WhereEquals(WhereParams whereParams)
		{
			EnsureValidFieldName(whereParams);
			var transformToEqualValue = TransformToEqualValue(whereParams);
			lastEquality = new KeyValuePair<string, string>(whereParams.FieldName, transformToEqualValue);
			if (theQueryText.Length > 0 && theQueryText[theQueryText.Length - 1] != '(')
			{
				theQueryText.Append(" ");
			}

			NegateIfNeeded();

			theQueryText.Append(whereParams.FieldName);
			theQueryText.Append(":");
			theQueryText.Append(transformToEqualValue);
		}

		private string EnsureValidFieldName(WhereParams whereParams)
		{
			if (theSession == null || theSession.Conventions == null || whereParams.IsNestedPath)
				return whereParams.FieldName;

			var identityProperty = theSession.Conventions.GetIdentityProperty(typeof(T));
			if (identityProperty == null || identityProperty.Name != whereParams.FieldName)
				return whereParams.FieldName;
			
			return whereParams.FieldName = Constants.DocumentIdFieldName;
		}

		///<summary>
		/// Negate the next operation
		///</summary>
		public void NegateNext()
		{
			negate = !negate;
		}

		private void NegateIfNeeded()
		{
			if (negate == false)
				return;
			negate = false;
			theQueryText.Append("-");
		}

		/// <summary>
		///   Avoid using WhereContains(), use Search() instead
		/// </summary>
		[Obsolete("Avoid using WhereContains(), use Search() instead")]
		public void WhereContains(string fieldName, object value)
		{
			WhereEquals(new WhereParams
			{
				AllowWildcards = true,
				IsAnalyzed = true,
				FieldName = fieldName,
				Value = value
			});
		}

		/// <summary>
		///   Avoid using WhereContains(), use Search() instead
		/// </summary>
		[Obsolete("Avoid using WhereContains(), use Search() instead")]
		public void WhereContains(string fieldName, params object[] values)
		{
			if (values == null || values.Length == 0)
			{
				WhereEquals(fieldName, "Empty_Contains_" + Guid.NewGuid());
				return;
			}

			OpenSubclause();

			WhereContains(fieldName, values[0]);


			for (var i = 1; i < values.Length; i++)
			{
				OrElse();
				WhereContains(fieldName, values[i]);
			}

			CloseSubclause();
		}

		/// <summary>
		/// Check that the field has one of the specified value
		/// </summary>
		public void WhereIn(string fieldName, IEnumerable<object> values)
		{
			bool first = true;

			OpenSubclause();

			foreach (var value in values)
			{
				if (first == false)
				{
					OrElse();
				}

				first = false;

				WhereEquals(new WhereParams
				{
					AllowWildcards = true,
					IsAnalyzed = true,
					FieldName = fieldName,
					Value = value
				});

				
			}

			if(first) // no items
				WhereEquals(fieldName, "Empty_In_" + Guid.NewGuid());

			CloseSubclause();
		}

		/// <summary>
		///   Avoid using WhereContains(), use Search() instead
		/// </summary>
		[Obsolete("Avoid using WhereContains(), use Search() instead")]
		public void WhereContains(string fieldName, IEnumerable<object> values)
		{
			WhereContains(fieldName, values.ToArray());
		}

		/// <summary>
		///   Matches fields which starts with the specified value.
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		public void WhereStartsWith(string fieldName, object value)
		{
			// NOTE: doesn't fully match StartsWith semantics
			WhereEquals(
				new WhereParams
				{
					FieldName = fieldName,
					Value = String.Concat(value, "*"),
					IsAnalyzed = true,
					AllowWildcards = true
				});
		}

		/// <summary>
		///   Matches fields which ends with the specified value.
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		public void WhereEndsWith(string fieldName, object value)
		{
			// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Wildcard%20Searches
			// You cannot use a * or ? symbol as the first character of a search

			// NOTE: doesn't fully match EndsWith semantics
			WhereEquals(
				new WhereParams
				{
					FieldName = fieldName,
					Value = String.Concat("*", value),
					AllowWildcards = true,
					IsAnalyzed = true
				});
		}

		/// <summary>
		///   Matches fields where the value is between the specified start and end, exclusive
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "start">The start.</param>
		/// <param name = "end">The end.</param>
		/// <returns></returns>
		public void WhereBetween(string fieldName, object start, object end)
		{
			if (theQueryText.Length > 0)
			{
				theQueryText.Append(" ");
			}

			if ((start ?? end) != null)
				sortByHints.Add(new KeyValuePair<string, Type>(fieldName, (start ?? end).GetType()));

			NegateIfNeeded();

			fieldName = GetFieldNameForRangeQueries(fieldName, start, end);

			theQueryText.Append(fieldName).Append(":{");
			theQueryText.Append(start == null ? "*" : TransformToRangeValue(new WhereParams{Value = start, FieldName = fieldName}));
			theQueryText.Append(" TO ");
			theQueryText.Append(end == null ? "NULL" : TransformToRangeValue(new WhereParams{Value = end, FieldName = fieldName}));
			theQueryText.Append("}");
		}

		/// <summary>
		///   Matches fields where the value is between the specified start and end, inclusive
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "start">The start.</param>
		/// <param name = "end">The end.</param>
		/// <returns></returns>
		public void WhereBetweenOrEqual(string fieldName, object start, object end)
		{
			if (theQueryText.Length > 0)
			{
				theQueryText.Append(" ");
			}
			if ((start ?? end) != null)
				sortByHints.Add(new KeyValuePair<string, Type>(fieldName, (start ?? end).GetType()));

			NegateIfNeeded();

			fieldName = GetFieldNameForRangeQueries(fieldName, start, end);

			theQueryText.Append(fieldName).Append(":[");
			theQueryText.Append(start == null ? "*" : TransformToRangeValue(new WhereParams { Value = start, FieldName = fieldName }));
			theQueryText.Append(" TO ");
			theQueryText.Append(end == null ? "NULL" : TransformToRangeValue(new WhereParams { Value = end, FieldName = fieldName }));
			theQueryText.Append("]");
		}

		private string GetFieldNameForRangeQueries(string fieldName, object start, object end)
		{
			fieldName = EnsureValidFieldName(new WhereParams {FieldName = fieldName});

			if(fieldName == Constants.DocumentIdFieldName)
				return fieldName;

			var val = (start ?? end);
			var isNumeric = val is int || val is long || val is decimal || val is double || val is float;

			if (isNumeric && fieldName.EndsWith("_Range") == false)
				fieldName = fieldName + "_Range";
			return fieldName;
		}

		/// <summary>
		///   Matches fields where the value is greater than the specified value
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		public void WhereGreaterThan(string fieldName, object value)
		{
			WhereBetween(fieldName, value, null);
		}

		/// <summary>
		///   Matches fields where the value is greater than or equal to the specified value
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		public void WhereGreaterThanOrEqual(string fieldName, object value)
		{
			WhereBetweenOrEqual(fieldName, value, null);
		}

		/// <summary>
		///   Matches fields where the value is less than the specified value
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		public void WhereLessThan(string fieldName, object value)
		{
			WhereBetween(fieldName, null, value);
		}

		/// <summary>
		///   Matches fields where the value is less than or equal to the specified value
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		public void WhereLessThanOrEqual(string fieldName, object value)
		{
			WhereBetweenOrEqual(fieldName, null, value);
		}

		/// <summary>
		///   Add an AND to the query
		/// </summary>
		public void AndAlso()
		{
			if (theQueryText.Length < 1)
			{
				throw new InvalidOperationException("Missing where clause");
			}

			theQueryText.Append(" AND");
		}

		/// <summary>
		///   Add an OR to the query
		/// </summary>
		public void OrElse()
		{
			if (theQueryText.Length < 1)
			{
				throw new InvalidOperationException("Missing where clause");
			}

			theQueryText.Append(" OR");
		}

		/// <summary>
		///   Specifies a boost weight to the last where clause.
		///   The higher the boost factor, the more relevant the term will be.
		/// </summary>
		/// <param name = "boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
		/// <returns></returns>
		/// <remarks>
		///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
		/// </remarks>
		public void Boost(decimal boost)
		{
			if (theQueryText.Length < 1)
			{
				throw new InvalidOperationException("Missing where clause");
			}

			if (boost <= 0m)
			{
				throw new ArgumentOutOfRangeException("boost","Boost factor must be a positive number");
			}

			if (boost != 1m)
			{
				// 1.0 is the default
				theQueryText.Append("^").Append(boost);
			}
		}

		/// <summary>
		///   Specifies a fuzziness factor to the single word term in the last where clause
		/// </summary>
		/// <param name = "fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
		/// <returns></returns>
		/// <remarks>
		///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
		/// </remarks>
		public void Fuzzy(decimal fuzzy)
		{
			if (theQueryText.Length < 1)
			{
				throw new InvalidOperationException("Missing where clause");
			}

			if (fuzzy < 0m || fuzzy > 1m)
			{
				throw new ArgumentOutOfRangeException("Fuzzy distance must be between 0.0 and 1.0");
			}

			var ch = theQueryText[theQueryText.Length - 1];
			if (ch == '"' || ch == ']')
			{
				// this check is overly simplistic
				throw new InvalidOperationException("Fuzzy factor can only modify single word terms");
			}

			theQueryText.Append("~");
			if (fuzzy != 0.5m)
			{
				// 0.5 is the default
				theQueryText.Append(fuzzy);
			}
		}

		/// <summary>
		///   Specifies a proximity distance for the phrase in the last where clause
		/// </summary>
		/// <param name = "proximity">number of words within</param>
		/// <returns></returns>
		/// <remarks>
		///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
		/// </remarks>
		public void Proximity(int proximity)
		{
			if (theQueryText.Length < 1)
			{
				throw new InvalidOperationException("Missing where clause");
			}

			if (proximity < 1)
			{
				throw new ArgumentOutOfRangeException("proximity", "Proximity distance must be positive number");
			}

			if (theQueryText[theQueryText.Length - 1] != '"')
			{
				// this check is overly simplistic
				throw new InvalidOperationException("Proximity distance can only modify a phrase");
			}

			theQueryText.Append("~").Append(proximity);
		}

		/// <summary>
		///   Order the results by the specified fields
		///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
		///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
		/// </summary>
		/// <param name = "fields">The fields.</param>
		public void OrderBy(params string[] fields)
		{
			orderByFields = orderByFields.Concat(fields).ToArray();
		}

		/// <summary>
		///   Instructs the query to wait for non stale results as of now.
		/// </summary>
		/// <returns></returns>
		public void WaitForNonStaleResultsAsOfNow()
		{
			theWaitForNonStaleResults = true;
			cutoff = SystemTime.UtcNow;
			timeout = DefaultTimeout;
		}

		/// <summary>
		///   Instructs the query to wait for non stale results as of now for the specified timeout.
		/// </summary>
		/// <param name = "waitTimeout">The wait timeout.</param>
		/// <returns></returns>
		IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
		{
			WaitForNonStaleResultsAsOfNow(waitTimeout);
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
		/// current document store.
		/// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
		/// </summary>
		IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfLastWrite()
		{
			WaitForNonStaleResultsAsOfLastWrite();
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
		/// current document store.
		/// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
		/// </summary>
		IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfLastWrite(TimeSpan waitTimeout)
		{
			WaitForNonStaleResultsAsOfLastWrite(waitTimeout);
			return this;
		}

		/// <summary>
		///   Instructs the query to wait for non stale results as of the cutoff date.
		/// </summary>
		/// <param name = "cutOff">The cut off.</param>
		/// <returns></returns>
		IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(DateTime cutOff)
		{
			WaitForNonStaleResultsAsOf(cutOff);
			return this;
		}

		/// <summary>
		///   Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
		/// </summary>
		/// <param name = "cutOff">The cut off.</param>
		/// <param name = "waitTimeout">The wait timeout.</param>
		IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(DateTime cutOff,
																						   TimeSpan waitTimeout)
		{
			WaitForNonStaleResultsAsOf(cutOff, waitTimeout);
			return this;
		}

		/// <summary>
		///   Instructs the query to wait for non stale results as of now.
		/// </summary>
		/// <returns></returns>
		IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfNow()
		{
			WaitForNonStaleResultsAsOfNow();
			return this;
		}

		/// <summary>
		///   Instructs the query to wait for non stale results as of now for the specified timeout.
		/// </summary>
		/// <param name = "waitTimeout">The wait timeout.</param>
		/// <returns></returns>
		public void WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
		{
			theWaitForNonStaleResults = true;
			cutoff = SystemTime.UtcNow;
			timeout = waitTimeout;
		}

		/// <summary>
		///   Instructs the query to wait for non stale results as of the cutoff date.
		/// </summary>
		/// <param name = "cutOff">The cut off.</param>
		/// <returns></returns>
		public void WaitForNonStaleResultsAsOf(DateTime cutOff)
		{
			WaitForNonStaleResultsAsOf(cutOff, DefaultTimeout);
		}

		/// <summary>
		///   Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
		/// </summary>
		/// <param name = "cutOff">The cut off.</param>
		/// <param name = "waitTimeout">The wait timeout.</param>
		public void WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout)
		{
			theWaitForNonStaleResults = true;
			cutoff = cutOff.ToUniversalTime();
			timeout = waitTimeout;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
		/// current document store.
		/// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
		/// </summary>
		public void WaitForNonStaleResultsAsOfLastWrite()
		{
			WaitForNonStaleResultsAsOfLastWrite(DefaultTimeout);
		}
		/// <summary>
		/// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
		/// current document store.
		/// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
		/// </summary>
		public void WaitForNonStaleResultsAsOfLastWrite(TimeSpan waitTimeout)
		{
			theWaitForNonStaleResults = true;
			timeout = waitTimeout;
			cutoffEtag = theSession.DocumentStore.GetLastWrittenEtag();
		}

		/// <summary>
		///   EXPERT ONLY: Instructs the query to wait for non stale results.
		///   This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		public void WaitForNonStaleResults()
		{
			WaitForNonStaleResults(DefaultTimeout);
		}

		/// <summary>
		/// Provide statistics about the query, such as total count of matching records
		/// </summary>
		public void Statistics(out RavenQueryStatistics stats)
		{
			stats = queryStats;
		}

		/// <summary>
		/// Callback to get the results of the query
		/// </summary>
		public void AfterQueryExecuted(Action<QueryResult> afterQueryExecutedCallback)
		{
			this.afterQueryExecutedCallback += afterQueryExecutedCallback;
		}

		/// <summary>
		/// Called externally to raise the after query executed callback
		/// </summary>
		public void InvokeAfterQueryExecuted(QueryResult result)
		{
			var queryExecuted = afterQueryExecutedCallback;
			if (queryExecuted != null)
				queryExecuted(result);
		}

		#endregion

#if !NET_3_5
		protected virtual Task<QueryOperation> ExecuteActualQueryAsync()
		{
			using(queryOperation.EnterQueryContext())
			{
				queryOperation.LogQuery();
				return theAsyncDatabaseCommands.QueryAsync(indexName, queryOperation.IndexQuery, includes.ToArray())
					.ContinueWith(task =>
					{
						if (queryOperation.IsAcceptable(task.Result) == false)
						{
							return TaskDelay(100)
								.ContinueWith(_ => ExecuteActualQueryAsync())
								.Unwrap();
						}
						InvokeAfterQueryExecuted(queryOperation.CurrentQueryResults);
						return Task.Factory.StartNew(() => queryOperation);
					}).Unwrap();
			}
		}

		private static Task TaskDelay(int dueTimeMilliseconds)
		{
			var taskComplectionSource = new TaskCompletionSource<object>();
			var cancellationTokenRegistration = new CancellationTokenRegistration();
			var timer = new Timer(o =>
			{
				cancellationTokenRegistration.Dispose();
				((Timer)o).Dispose();
				taskComplectionSource.TrySetResult(null);
			});
			timer.Change(dueTimeMilliseconds, -1);
			return taskComplectionSource.Task;
		}

		private static Task<TResult> TaskResult<TResult>(TResult value)
		{
			var taskComplectionSource = new TaskCompletionSource<TResult>();
			taskComplectionSource.SetResult(value);
			return taskComplectionSource.Task;
		}
#endif
	 
		/// <summary>
		///   Generates the index query.
		/// </summary>
		/// <param name = "query">The query.</param>
		/// <returns></returns>
		protected virtual IndexQuery GenerateIndexQuery(string query)
		{
			if(isSpatialQuery)
			{
				return new SpatialIndexQuery
				{
					GroupBy = groupByFields,
					AggregationOperation = aggregationOp,
					Query = query,
					PageSize = pageSize ?? 128,
					Start = start,
					Cutoff = cutoff,
					CutoffEtag = cutoffEtag,
					SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
					FieldsToFetch = projectionFields,
					Latitude = lat,
					Longitude = lng,
					Radius = radius,
					DefaultField = defaultField
				};
			}

			return new IndexQuery
			{
				GroupBy = groupByFields,
				AggregationOperation = aggregationOp,
				Query = query,
				PageSize = pageSize ?? 128,
				Start = start,
				Cutoff = cutoff,
				CutoffEtag = cutoffEtag,
				SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
				FieldsToFetch = projectionFields,
				DefaultField = defaultField
			};
		}

		private static readonly Regex espacePostfixWildcard = new Regex(@"\\\*(\s|$)", 
#if !SILVERLIGHT
			RegexOptions.Compiled
#else
			RegexOptions.None
#endif

			);

		/// <summary>
		/// Perform a search for documents which fields that match the searchTerms.
		/// If there is more than a single term, each of them will be checked independently.
		/// </summary>
		public void Search(string fieldName, string searchTerms, EscapeQueryOptions escapeQueryOptions = EscapeQueryOptions.RawQuery)
		{
			lastEquality = new KeyValuePair<string, string>(fieldName, "<<"+searchTerms+">>");
			theQueryText.Append(' ');
			
			NegateIfNeeded();
			switch (escapeQueryOptions)
			{
				case EscapeQueryOptions.EscapeAll:
					searchTerms = RavenQuery.Escape(searchTerms, false, false);
					break;
				case EscapeQueryOptions.AllowPostfixWildcard:
					searchTerms = RavenQuery.Escape(searchTerms, false, false);
					searchTerms = espacePostfixWildcard.Replace(searchTerms, "*");
					break;
				case EscapeQueryOptions.AllowAllWildcards:
					searchTerms = RavenQuery.Escape(searchTerms, false, false);
					searchTerms = searchTerms.Replace("\\*", "*");
					break;
				case EscapeQueryOptions.RawQuery:
					break;
				default:
					throw new ArgumentOutOfRangeException("escapeQueryOptions", "Value: "  + escapeQueryOptions);
			}

			theQueryText.Append(fieldName).Append(":").Append("<<").Append(searchTerms).Append(">>");
		}

		private string TransformToEqualValue(WhereParams whereParams)
		{
			if (whereParams.Value == null)
			{
				return Constants.NullValueNotAnalyzed;
			}
			if(Equals(whereParams.Value, string.Empty))
			{
				return Constants.EmptyStringNotAnalyzed;
			}

			var type = TypeSystem.GetNonNullableType(whereParams.Value.GetType());

			if (type == typeof(bool))
			{
				return (bool)whereParams.Value ? "true" : "false";
			}

			if (type == typeof(DateTime))
			{
				return DateTools.DateToString(((DateTime)whereParams.Value), DateTools.Resolution.MILLISECOND);
			}
			
			if(type == typeof(decimal))
			{
				return RavenQuery.Escape(((double)((decimal)whereParams.Value)).ToString(CultureInfo.InvariantCulture), false, false);
			}

			if (type == typeof(DateTimeOffset))
			{
				return DateTools.DateToString(((DateTimeOffset)whereParams.Value).UtcDateTime, DateTools.Resolution.MILLISECOND);
			}

			if(whereParams.FieldName == Constants.DocumentIdFieldName && whereParams.Value is string == false)
			{
				return theSession.Conventions.FindFullDocumentKeyFromNonStringIdentifier(whereParams.Value, typeof(T), false);
			}

			var escaped = RavenQuery.Escape(Convert.ToString(whereParams.Value, CultureInfo.InvariantCulture),
											whereParams.AllowWildcards && whereParams.IsAnalyzed, true);

			if (whereParams.Value is string == false)
				return escaped;

			return whereParams.IsAnalyzed ? escaped : String.Concat("[[", escaped, "]]");
		}

		private string TransformToRangeValue(WhereParams whereParams)
		{
			if (whereParams.Value == null)
				return Constants.NullValueNotAnalyzed;
			if (Equals(whereParams.Value, string.Empty))
				return Constants.EmptyStringNotAnalyzed;

			if (whereParams.Value is DateTime)
				return DateTools.DateToString(((DateTime)whereParams.Value), DateTools.Resolution.MILLISECOND);
			if (whereParams.Value is DateTimeOffset)
				return DateTools.DateToString(((DateTimeOffset)whereParams.Value).UtcDateTime, DateTools.Resolution.MILLISECOND);

			if (whereParams.FieldName == Constants.DocumentIdFieldName && whereParams.Value is string == false)
			{
				return theSession.Conventions.FindFullDocumentKeyFromNonStringIdentifier(whereParams.Value, typeof(T), false);
			}
			if (whereParams.Value is int)
				return NumberUtil.NumberToString((int)whereParams.Value);
			if (whereParams.Value is long)
				return NumberUtil.NumberToString((long)whereParams.Value);
			if (whereParams.Value is decimal)
				return NumberUtil.NumberToString((double)(decimal)whereParams.Value);
			if (whereParams.Value is double)
				return NumberUtil.NumberToString((double)whereParams.Value);
			if (whereParams.Value is float)
				return NumberUtil.NumberToString((float)whereParams.Value);
		   

			return RavenQuery.Escape(whereParams.Value.ToString(), false, true);
		}

		/// <summary>
		///   Returns a <see cref = "System.String" /> that represents this instance.
		/// </summary>
		/// <returns>
		///   A <see cref = "System.String" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			if (currentClauseDepth != 0)
			{
				throw new InvalidOperationException(
					string.Format("A clause was not closed correctly within this query, current clause depth = {0}",
								  currentClauseDepth));
			}
			return theQueryText.ToString().Trim();
		}

		/// <summary>
		///   The last term that we asked the query to use equals on
		/// </summary>
		public KeyValuePair<string, string> GetLastEqualityTerm()
		{
			return lastEquality;
		}

		public void Intersect()
		{
			theQueryText.Append(Constants.IntersectSeperator);
		}

		/// <summary>
		/// Order the search results randomly
		/// </summary>
		IDocumentQueryCustomization IDocumentQueryCustomization.RandomOrdering()
		{
			RandomOrdering();
			return this;
		}

		/// <summary>
		/// Order the search results randomly using the specified seed
		/// this is useful if you want to have repeatable random queries
		/// </summary>
		IDocumentQueryCustomization IDocumentQueryCustomization.RandomOrdering(string seed)
		{
			RandomOrdering(seed);
			return this;
		}

#if !NET_3_5
		/// <summary>
		/// Returns a list of results for a query asynchronously. 
		/// </summary>
		public Task<Tuple<QueryResult,IList<T>>> ToListAsync()
		{
			return InitAsync()
				.ContinueWith(t => ProcessEnumerator(t))
				.Unwrap()
				.ContinueWith(t => Tuple.Create(t.Result.Item1.CurrentQueryResults, t.Result.Item2));
		}

		/// <summary>
		/// Gets the total count of records for this query
		/// </summary>
		public Task<int> CountAsync()
		{
			Take(0);
			return QueryResultAsync
				.ContinueWith(r => r.Result.TotalResults);
		}
#endif
	}
}