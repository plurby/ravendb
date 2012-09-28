﻿using System;
using System.Globalization;
using Newtonsoft.Json;

namespace Raven.Abstractions.Json
{
	public class JsonNumericConverter<T> : JsonConverter where T : struct
	{
		public delegate bool TryParse(string s, NumberStyles styles, IFormatProvider provider, out T val);

		private readonly TryParse tryParse;

		public JsonNumericConverter(TryParse tryParse)
		{
			this.tryParse = tryParse;
		}

		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			writer.WriteValue(value);
		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			var s = reader.Value as string;

			if (s != null)
			{
				T val;
				if (tryParse(s, NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture,
						 out val))
					return val;
			}
			if (reader.Value == null)
				return null;
			try
			{
				return Convert.ChangeType(reader.Value, typeof(T), CultureInfo.InvariantCulture);
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Could not convert '" + reader.Value + "' to " + objectType.Name, e);
			}
		}

		public override bool CanConvert(Type objectType)
		{
			return typeof (T) == objectType || typeof (T?) == objectType;
		}

		public override bool CanWrite
		{
			get { return false; }
		}
	}
}