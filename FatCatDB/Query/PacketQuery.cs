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
    /// <summary>
    /// This class encapsulates those parts of the full query, which were
    /// not used in determining the path of the packet.
    /// Therefore these query directives can be used for filtering the
    /// records inside a packet.
    /// </summary>
    /// <typeparam name="T">An annotated class of a database table record</typeparam>
    internal class PacketQuery<T> where T : class, new() {
        internal TableIndex<T> TableIndex { get; }

        /// <summary>
        /// The original, full query.
        /// </summary>
        internal QueryBase<T> Query { get; }
        internal Dictionary<int, PathFilter<T>> PathFilters { get; } = new Dictionary<int, PathFilter<T>>();
        internal List<Tuple<int, SortingDirection>> Sorting { get; } = new List<Tuple<int, SortingDirection>>();

        internal PacketQuery(TableIndex<T> index, QueryBase<T> query) {
            this.TableIndex = index;
            this.Query = query;

            /*
                Find free filters
            */
            foreach(var item in query.PathFilters) {
                if (!index.PropertyIndices.Contains(item.Key)) {
                    this.PathFilters[item.Key] = item.Value;
                }
            }

            /*
                Find free sorting directives
            */
            foreach(var sortingDirective in query.Sorting) {
                if (!index.PropertyIndices.Contains(sortingDirective.Item1)) {
                    this.Sorting.Add(sortingDirective);
                }
            }
        }
    }
}
