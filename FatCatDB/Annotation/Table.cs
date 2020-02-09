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

namespace FatCatDB.Annotation {
    /// <summary>
    /// To annotate database tables
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class Table : Attribute {
        /// <summary>
        /// The TSV name of the database table
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// A comma seperated list of columns which define a primary key.
        /// Note that the order is important, and that this composite has
        /// to be unique only inside a packet. (It's not a problem when
        /// it's globally unique.)
        /// </summary>
        public string Unique { get; set; }

        /// <summary>
        /// A free text to describe the table. Optional.
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// This is the text representation of the value NULL
        /// </summary>
        public string NullValue { get; set; } = "";
    }
}
