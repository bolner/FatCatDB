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

namespace FatCatDB {
    internal class TsvMapping<T> where T : class, new() {
        /// <summary>
        /// Mapping TSV column indices to property indices
        /// </summary>
        public Nullable<int>[] FromTsvToRecord { get; }

        /// <summary>
        /// Mapping property indices to TSV column indices
        /// </summary>
        public Nullable<int>[] FromRecordToTsv { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="table">A database table</param>
        /// <param name="header">The header of a TSV file</param>
        public TsvMapping(Table<T> table, List<string> header) {
            Dictionary<string, Nullable<int>> headerIndex = new Dictionary<string, Nullable<int>>();
            Dictionary<string, Nullable<int>> columnIndex = new Dictionary<string, Nullable<int>>();
            FromTsvToRecord = new Nullable<int>[header.Count];
            FromRecordToTsv = new Nullable<int>[table.ColumnNames.Length];

            /*
                Explore
            */
            int index = 0;
            foreach(string name in table.ColumnNames) {
                columnIndex[name] = index;
                index++;
            }

            index = 0;
            foreach(string name in header) {
                headerIndex[name] = index;
                index++;
            }

            /*
                Connect
            */
            foreach(string name in table.ColumnNames) {
                Nullable<int> from = columnIndex[name];
                FromRecordToTsv[(int)from] = null;
                headerIndex.TryGetValue(name, out FromRecordToTsv[(int)from]);
            }

            foreach(string name in header) {
                Nullable<int> from = headerIndex[name];
                FromTsvToRecord[(int)from] = null;
                columnIndex.TryGetValue(name, out FromTsvToRecord[(int)from]);
            }
        }
    }
}
