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

namespace FatCatDB {
    /// <summary>
    /// Base class for the DB context classes.
    /// Derive your own DB contexts from this class.
    /// Define the tables as class properties. (see examples in README.md)
    /// </summary>
    public class DbContextBase {
        private List<TableBase> tables = new List<TableBase>();

        /// <summary>
        /// The type converter is responsible for serializing and
        /// de-serializing data of different types.
        /// </summary>
        internal TypeConverter TypeConverter { get; }
        internal Configuration Configuration { get; }
        private DateTimeZone timeZoneUTC = DateTimeZoneProviders.Tzdb["Etc/UTC"];

        /// <summary>
        /// Returns the current date/time in UTC time zone.
        /// </summary>
        public LocalDateTime NowUTC {
            get {
                return SystemClock.Instance.GetCurrentInstant().InZone(
                    timeZoneUTC
                ).LocalDateTime;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public DbContextBase() {
            var typeConverterSetup = new TypeConverterSetup();
            var configurator = new Configurator();

            this.OnConfiguring(typeConverterSetup, configurator);
            
            this.TypeConverter = new TypeConverter(typeConverterSetup);
            this.Configuration = new Configuration(configurator);

            var props = this.GetType().GetProperties();
            foreach(var prop in props) {
                var value = prop.GetValue(this);
                
                if (value is TableBase) {
                    var table = (TableBase)value;
                    table.Initialize(this);
                    this.tables.Add(table);
                }
            }
        }

        /// <summary>
        /// Override this method in the child classes to change
        /// the configuration or to add new types.
        /// </summary>
        /// <param name="typeConverter">Use this to add new types</param>
        /// <param name="configurator">For changing the configuration</param>
        protected virtual void OnConfiguring(TypeConverterSetup typeConverter, Configurator configurator) {

        }
    }
}
