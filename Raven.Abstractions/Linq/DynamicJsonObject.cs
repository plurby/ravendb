//-----------------------------------------------------------------------
// <copyright file="DynamicJsonObject.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET_3_5
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Abstractions.Linq
{
	public interface IDynamicJsonObject
	{
		/// <summary>
		/// Gets the inner json object
		/// </summary>
		/// <value>The inner.</value>
		RavenJObject Inner { get; }

	}
	/// <summary>
	/// A dynamic implementation on top of <see cref="RavenJObject"/>
	/// </summary>
	public class DynamicJsonObject : DynamicObject, IEnumerable<object>, IDynamicJsonObject
	{
		private DynamicJsonObject parent;

		public IEnumerator<object> GetEnumerator()
		{
			return
				(
					from item in inner
					where item.Key[0] != '$'
					select new KeyValuePair<string, object>(item.Key, TransformToValue(item.Value))
				)
				.Cast<object>().GetEnumerator();
		}

		/// <summary>
		/// Returns a <see cref="T:System.String"/> that represents the current <see cref="T:System.Object"/>.
		/// </summary>
		/// <returns>
		/// A <see cref="T:System.String"/> that represents the current <see cref="T:System.Object"/>.
		/// </returns>
		/// <filterpriority>2</filterpriority>
		public override string ToString()
		{
			return inner.ToString();
		}

		/// <summary>
		/// Determines whether the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>.
		/// </summary>
		/// <returns>
		/// true if the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>; otherwise, false.
		/// </returns>
		/// <param name="other">The <see cref="T:System.Object"/> to compare with the current <see cref="T:System.Object"/>. </param><filterpriority>2</filterpriority>
		public override bool Equals(object other)
		{
			var dynamicJsonObject = other as DynamicJsonObject;
			if (dynamicJsonObject != null)
				return RavenJToken.DeepEquals(inner, dynamicJsonObject.inner);
			return base.Equals(other);
		}

		/// <summary>
		/// Serves as a hash function for a particular type. 
		/// </summary>
		/// <returns>
		/// A hash code for the current <see cref="T:System.Object"/>.
		/// </returns>
		/// <filterpriority>2</filterpriority>
		public override int GetHashCode()
		{
			return RavenJToken.GetDeepHashCode(inner);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		private readonly RavenJObject inner;

		/// <summary>
		/// Initializes a new instance of the <see cref="DynamicJsonObject"/> class.
		/// </summary>
		/// <param name="inner">The obj.</param>
		public DynamicJsonObject(RavenJObject inner)
		{
			this.inner = inner;
		}


		internal DynamicJsonObject(DynamicJsonObject parent, RavenJObject inner)
		{
			this.parent = parent;
			this.inner = inner;
		}

		/// <summary>
		/// Provides the implementation for operations that get member values. Classes derived from the <see cref="T:System.Dynamic.DynamicObject"/> class can override this method to specify dynamic behavior for operations such as getting a value for a property.
		/// </summary>
		/// <param name="binder">Provides information about the object that called the dynamic operation. The binder.Name property provides the name of the member on which the dynamic operation is performed. For example, for the Console.WriteLine(sampleObject.SampleProperty) statement, where sampleObject is an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject"/> class, binder.Name returns "SampleProperty". The binder.IgnoreCase property specifies whether the member name is case-sensitive.</param>
		/// <param name="result">The result of the get operation. For example, if the method is called for a property, you can assign the property value to <paramref name="result"/>.</param>
		/// <returns>
		/// true if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a run-time exception is thrown.)
		/// </returns>
		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			result = GetValue(binder.Name);
			return true;
		}

		/// <summary>
		/// Provides the implementation for operations that get a value by index. Classes derived from the <see cref="T:System.Dynamic.DynamicObject"/> class can override this method to specify dynamic behavior for indexing operations.
		/// </summary>
		/// <returns>
		/// true if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a run-time exception is thrown.)
		/// </returns>
		public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
		{
			if (indexes.Length != 1 || indexes[0] is string == false)
			{
				result = null;
				return false;
			}
			result = GetValue((string)indexes[0]);
			return true;
		}

		public object TransformToValue(RavenJToken jToken)
		{
			switch (jToken.Type)
			{
				case JTokenType.Object:
					var jObject = (RavenJObject)jToken;
					var values = jObject.Value<RavenJArray>("$values");
					if (values != null)
					{
						return new DynamicList(this, values.Select(TransformToValue).ToArray());
					}
					var refId = jObject.Value<string>("$ref");
					if (refId != null)
					{
						var ravenJObject = FindReference(refId);
						if (ravenJObject != null)
							return new DynamicJsonObject(this, ravenJObject);
					}
					return new DynamicJsonObject(this, jObject);
				case JTokenType.Array:
					var ar = (RavenJArray)jToken;
					return new DynamicList(this, ar.Select(TransformToValue).ToArray());
				case JTokenType.Date:
					return jToken.Value<DateTime>();
				case JTokenType.Null:
					return new DynamicNullObject { IsExplicitNull = true };
				default:
					var value = jToken.Value<object>();
					if (value is long)
					{
						var l = (long)value;
						if (l > int.MinValue && int.MaxValue > l)
							return (int)l;
					}
					var s = value as string;
					if (s != null)
					{
						DateTime dateTime;
						if (DateTime.TryParseExact(s, Default.OnlyDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTime))
						{
							return dateTime;
						}
						DateTimeOffset dateTimeOffset;
						if (DateTimeOffset.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTimeOffset))
						{
							return dateTimeOffset;
						}
					}
					return value ?? new DynamicNullObject { IsExplicitNull = true };
			}
		}

		private RavenJObject FindReference(string refId)
		{
			var p = this;
			while (p.parent != null)
				p = p.parent;

			return p.Scan().FirstOrDefault(x => x.Value<string>("$id") == refId);
		}

		private IEnumerable<RavenJObject> Scan()
		{
			var objs = new List<RavenJObject>();
			var lists = new List<RavenJArray>();

			objs.Add(inner);

			while (objs.Count > 0 || lists.Count > 0)
			{
				var objCopy = objs;
				objs = new List<RavenJObject>();
				foreach (var obj in objCopy)
				{
					yield return obj;
					foreach (var property in obj.Properties)
					{
						switch (property.Value.Type)
						{
							case JTokenType.Object:
								objs.Add((RavenJObject)property.Value);
								break;
							case JTokenType.Array:
								lists.Add((RavenJArray)property.Value);
								break;
						}
					}
				}

				var listsCopy = lists;
				lists = new List<RavenJArray>();
				foreach (var list in listsCopy)
				{
					foreach (var item in list)
					{
						switch (item.Type)
						{
							case JTokenType.Object:
								objs.Add((RavenJObject)item);
								break;
							case JTokenType.Array:
								lists.Add((RavenJArray)item);
								break;
						}
					}
				}
			}
		}

		public IEnumerable<object> Select(Func<object, object> func)
		{
			return new DynamicList(parent, Enumerable.Select(this, func).ToArray());
		}

		public IEnumerable<object> SelectMany(Func<object, IEnumerable<object>> func)
		{
			return new DynamicList(parent, Enumerable.SelectMany(this, func).ToArray());
		}

		/// <summary>
		/// Gets the value for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		/// <returns></returns>
		public object GetValue(string name)
		{
			if (name == Constants.DocumentIdFieldName)
			{
				return GetDocumentId();
			}
			RavenJToken value;
			if (inner.TryGetValue(name, out value))
			{
				return TransformToValue(value);
			}
			if (name.StartsWith("_"))
			{
				if (inner.TryGetValue(name.Substring(1), out value))
				{
					return TransformToValue(value);
				}
			}
			if (name == "Id")
			{
				return GetDocumentId();
			}
			if (name == "Inner")
			{
				return inner;
			}
			return new DynamicNullObject();
		}

		/// <summary>
		/// Gets the document id.
		/// </summary>
		/// <returns></returns>
		public object GetDocumentId()
		{
			var metadata = inner["@metadata"] as RavenJObject;
			if (metadata != null && string.IsNullOrEmpty(metadata.Value<string>("@id")) == false)
			{
				var id = metadata.Value<string>("@id");
				return string.IsNullOrEmpty(id) ? (object)new DynamicNullObject() : id;
			}
			return inner.Value<string>(Constants.DocumentIdFieldName) ?? (object)new DynamicNullObject();
		}


		/// <summary>
		/// Gets the inner json object
		/// </summary>
		/// <value>The inner.</value>
		RavenJObject IDynamicJsonObject.Inner
		{
			get { return inner; }
		}
	}

	public class DynamicList : DynamicObject, IEnumerable<object>
	{
		private readonly DynamicJsonObject parent;
		private readonly IEnumerable<object> inner;

		public DynamicList(IEnumerable<object> inner)
		{
			this.inner = inner;
		}

		internal DynamicList(DynamicJsonObject parent, IEnumerable<object> inner)
			: this(inner)
		{
			this.parent = parent;
		}

		public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
		{
			switch (binder.Name)
			{
				case "Count":
					if (args.Length == 0)
					{
						result = Count;
						return true;
					}
					result = Enumerable.Count(this, (Func<object, bool>)args[0]);
					return true;
				case "DefaultIfEmpty":
					result = inner.DefaultIfEmpty(new DynamicNullObject());
					return true;
			}
			return base.TryInvokeMember(binder, args, out result);
		}

		private IEnumerable<dynamic> Enumerate()
		{
			foreach (var item in inner)
			{
				var ravenJObject = item as RavenJObject;
				if (ravenJObject != null)
					yield return new DynamicJsonObject(parent, ravenJObject);
				var ravenJArray = item as RavenJArray;
				if (ravenJArray != null)
					yield return new DynamicList(parent, ravenJArray.ToArray());
				yield return item;
			}
		}

		public dynamic First()
		{
			return Enumerate().First();
		}

		public dynamic First(Func<dynamic, bool> predicate)
		{
			return Enumerate().First(predicate);
		}

		public dynamic Any(Func<dynamic, bool> predicate)
		{
			return Enumerate().Any(predicate);
		}

		public dynamic All(Func<dynamic, bool> predicate)
		{
			return Enumerate().All(predicate);
		}

		public dynamic FirstOrDefault(Func<dynamic, bool> predicate)
		{
			return Enumerate().FirstOrDefault(predicate) ?? new DynamicNullObject();
		}

		public dynamic FirstOrDefault()
		{
			return Enumerate().FirstOrDefault() ?? new DynamicNullObject();
		}

		public dynamic Single(Func<dynamic, bool> predicate)
		{
			return Enumerate().Single(predicate);
		}

		public IEnumerable<dynamic> Distinct()
		{
			return new DynamicList(Enumerate().Distinct().ToArray());
		}

		public dynamic SingleOrDefault(Func<dynamic, bool> predicate)
		{
			return Enumerate().SingleOrDefault(predicate) ?? new DynamicNullObject();
		}

		public dynamic SingleOrDefault()
		{
			return Enumerate().SingleOrDefault() ?? new DynamicNullObject();
		}

		public IEnumerator<object> GetEnumerator()
		{
			return Enumerate().GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return Enumerate().GetEnumerator();
		}


		public void CopyTo(Array array, int index)
		{
			((ICollection)inner).CopyTo(array, index);
		}

		public object this[int index]
		{
			get { return inner.ElementAt(index); }
		}

		public bool Contains(object item)
		{
			return inner.Contains(item);
		}

		public int Count
		{
			get { return inner.Count(); }
		}

		public int Sum(Func<dynamic, int> aggregator)
		{
			return Enumerate().Sum(aggregator);
		}

		public decimal Sum(Func<dynamic, decimal> aggregator)
		{
			return Enumerate().Sum(aggregator);
		}

		public float Sum(Func<dynamic, float> aggregator)
		{
			return Enumerate().Sum(aggregator);
		}

		public double Sum(Func<dynamic, double> aggregator)
		{
			return Enumerate().Sum(aggregator);
		}

		public long Sum(Func<dynamic, long> aggregator)
		{
			return Enumerate().Sum(aggregator);
		}

		public dynamic Last()
		{
			return Enumerate().Last();
		}

		public dynamic LastOrDefault()
		{
			return Enumerate().LastOrDefault() ?? new DynamicNullObject();
		}

		public dynamic Last(Func<dynamic, bool> predicate)
		{
			return Enumerate().Last(predicate);
		}

		public dynamic LastOrDefault(Func<dynamic, bool> predicate)
		{
			return Enumerate().LastOrDefault(predicate) ?? new DynamicNullObject();
		}

		/// <summary>
		/// Gets the length.
		/// </summary>
		/// <value>The length.</value>
		public int Length
		{
			get { return inner.Count(); }
		}

		public IEnumerable<object> Select(Func<object, object> func)
		{
			return new DynamicList(parent, inner.Select(func));
		}

		public IEnumerable<object> SelectMany(Func<object, IEnumerable<object>> func)
		{
			return new DynamicList(parent, inner.SelectMany(func));
		}

		public IEnumerable<object> Where(Func<object, bool> func)
		{
			return new DynamicList(parent, inner.Where(func));
		}

		public dynamic DefaultIfEmpty(object defaultValue = null)
		{
			return inner.DefaultIfEmpty(defaultValue ?? new DynamicNullObject());
		}
	}

}
#endif