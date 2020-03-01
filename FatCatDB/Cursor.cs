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
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FatCatDB {
    /// <summary>
    /// You can iterate through query results using instances of this class
    /// </summary>
    /// <typeparam name="T">An annotated database record class</typeparam>
    public class Cursor<T> : IEnumerable<T> where T : class, new() {
        private Table<T> table;
        private QueryPlan<T> queryPlan;
        private QueryEngine<T> queryEngine;
        
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="query">An object, specifying the query</param>
        public Cursor(Query<T> query) {
            this.table = query.QueryBase.Table;
            this.queryPlan = new QueryPlan<T>(query.QueryBase);
            this.queryEngine = new QueryEngine<T>(queryPlan);
        }

        /// <summary>
        /// Returns an enumerator for the IEnumerable interface
        /// </summary>
        public IEnumerator<T> GetEnumerator() {
            T next;

            while ((next = queryEngine.FetchNextRecord()) != null) {
                yield return next;
            }
        }

        /// <summary>
        /// Returns an enumerator for the IEnumerable interface
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }

        /// <summary>
        /// Creates an "Exporter" object that can be used to export the data in different formats.
        /// </summary>
        public Exporter<T> GetExporter() {
            return new Exporter<T>(this.table, this);
        }

        /// <summary>
        /// Returns the next record in the result set, or null
        /// if there are no more records.
        /// </summary>
        public async Task<T> FetchNextAsync() {
            return await queryEngine.FetchNextRecordAsync();
        }

        /// <summary>
        /// Returns multiple records from the result set, or
        /// an empty list.
        /// </summary>
        /// <param name="count">The maximal number of items to return</param>
        public async Task<List<T>> FetchAsync(int count) {
            var result = new List<T>();
            T item;

            for(int i = 0; i < count; i++) {
                item = await queryEngine.FetchNextRecordAsync();
                if (item == null) {
                    break;
                }

                result.Add(item);
            }

            return result;
        }

        /// <summary>
        /// Returns the string representation of the bookmark.
        /// Using the bookmark one can continue a limited query
        /// at the point it stopped.
        /// </summary>
        public string GetBookmark() {
            var lastRecord = this.queryEngine.GetLastRecordFetched();

            var bookmark = new Bookmark();
            bookmark.AddFragment(
                this.table.Annotation.Name,
                this.queryPlan.BestIndex.Name,
                this.table.GetFullRecordPath(this.queryPlan.BestIndex, lastRecord)
            );

            return bookmark.ToString();
        }
    }
}
