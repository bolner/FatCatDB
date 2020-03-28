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
using System.Reflection;
using System.Collections.Generic;

namespace FatCatDB {
    /// <summary>
    /// An index of a database table
    /// </summary>
    /// <typeparam name="T">An annotated database table record class</typeparam>
    internal class TableIndex<T> where T : class, new() {
        internal string Name { get; }
        internal List<int> PropertyIndices { get; }
        internal int Rank { get; }

        public TableIndex(string name, List<int> propertyIndices, int rank) {
            this.Name = name;
            this.PropertyIndices = propertyIndices;
            this.Rank = rank;
        }
    }
}
