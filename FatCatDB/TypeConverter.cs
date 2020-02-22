/*
    Copyright 2020 Tamas Bolner
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
      http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
using System;
using System.Collections.Generic;
using NodaTime;
using NodaTime.Text;

namespace FatCatDB {
    /// <summary>
    /// For creating a type converter instance
    /// </summary>
    public class TypeConverterSetup {
        private Dictionary<string, Func<object, object>> converters =  new Dictionary<string, Func<object, object>>();
        private LocalDatePattern datePattern = LocalDatePattern.CreateWithInvariantCulture("yyyy-MM-dd");
        private LocalDateTimePattern dtPattern = LocalDateTimePattern.CreateWithInvariantCulture(
            "yyyy-MM-dd HH:mm:ss"
        );

        /// <summary>
        /// Constructor
        /// Sets up the default serialization for some basic types, which can
        /// be overwritten by the user.
        /// </summary>
        public TypeConverterSetup() {
            /*
                Default type conversions
            */
            this.RegisterTypeConverter<int, string>(x => x.ToString());
            this.RegisterTypeConverter<string, int>(x => Convert.ToInt32(x));

            this.RegisterTypeConverter<long, string>(x => x.ToString());
            this.RegisterTypeConverter<string, long>(x => Convert.ToInt64(x));

            this.RegisterTypeConverter<float, string>(x => x.ToString());
            this.RegisterTypeConverter<string, float>(x => Convert.ToSingle(x));

            this.RegisterTypeConverter<double, string>(x => x.ToString());
            this.RegisterTypeConverter<string, double>(x => Convert.ToDouble(x));

            this.RegisterTypeConverter<LocalDate, string>(x => datePattern.Format(x));
            this.RegisterTypeConverter<string, LocalDate>(x => datePattern.Parse(x).Value);

            this.RegisterTypeConverter<LocalDateTime, string>((x) => {
                    return dtPattern.Format(x);
            });

            this.RegisterTypeConverter<string, LocalDateTime>((x) => {
                    return dtPattern.Parse(x).Value;
            });
        }

        /// <summary>
        /// Add a new lambda function which converts between types.
        /// When you add a new type, always add two converters for it:
        ///     one that converts to string, and another backwards.
        /// </summary>
        /// <param name="converter">Lambda function that does the conversion</param>
        /// <typeparam name="SOURCE">Source type</typeparam>
        /// <typeparam name="DEST">Destination type</typeparam>
        public TypeConverterSetup RegisterTypeConverter<SOURCE, DEST>(Func<SOURCE, DEST> converter) {
            converters[$"{typeof(SOURCE).Name}|{typeof(DEST).Name}"]
                = new Func<object, object>(x => (DEST)converter((SOURCE)x));
            
            return this;
        }

        /// <summary>
        /// Returns all converters, indexed by source|destination strings.
        /// </summary>
        internal Dictionary<string, Func<object, object>> GetConverters() {
            return new Dictionary<string, Func<object, object>>(this.converters);
        }
    }

    /// <summary>
    /// The type converter is responsible for serializing and
    /// de-serializing data of different types.
    /// </summary>
    internal class TypeConverter {
        private Dictionary<string, Func<object, object>> converters =  new Dictionary<string, Func<object, object>>();

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="typeConverterSetup">Source of the settings</param>
        internal TypeConverter(TypeConverterSetup typeConverterSetup) {
            /*
                Copy the settings
            */
            this.converters = new Dictionary<string, Func<object, object>>(typeConverterSetup.GetConverters());
        }

        /// <summary>
        /// Converts between types
        /// </summary>
        /// <param name="sourceTypeName">The name of the source type</param>
        /// <param name="destTypeName">The name of the destination typoe</param>
        /// <param name="sourceValue">The value to convert</param>
        /// <returns>The value converted to a new type</returns>
        internal object ConvertType(string sourceTypeName, string destTypeName, object sourceValue) {
            Func<object, object> func = null;
            converters.TryGetValue($"{sourceTypeName}|{destTypeName}", out func);

            if (func == null) {
                throw new FatCatException($"Unable to convert from type '{sourceTypeName}' to type '{destTypeName}'."
                    + " Please add a custom type conversion function to FatCatDB. (See the docs for TypeConverter)");
            }

            return func(sourceValue);
        }

        /// <summary>
        /// Returns a lambda function for a specific type conversion.
        /// This lambda function had to be registered already.
        /// </summary>
        internal Func<object, object> GetConverter(string sourceTypeName, string destTypeName) {
            Func<object, object> func = null;
            converters.TryGetValue($"{sourceTypeName}|{destTypeName}", out func);

            if (func == null) {
                throw new FatCatException($"Unable to convert from type '{sourceTypeName}' to type '{destTypeName}'."
                    + " Please add a custom type conversion function to FatCatDB. (See the docs for TypeConverter)");
            }

            return func;
        }
    }
}
