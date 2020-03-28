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
using System.Collections.Concurrent;

namespace FatCatDB {
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T">An annotated class of a database table record</typeparam>
    internal class PacketCollector<T> : IEnumerable<Packet<T>> where T : class, new() {
        private Table<T> table;
        private List<TableIndex<T>> indices = new List<TableIndex<T>>();
        private ConcurrentDictionary<string, Tuple<TableIndex<T>, List<string>>> tracker
            = new ConcurrentDictionary<string, Tuple<TableIndex<T>, List<string>>>();

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="table">The table to which the records and packets belong to.</param>
        /// <param name="skipIndex">When collecting the packets, avoid the ones belonging to this index.</param>
        internal PacketCollector(Table<T> table, TableIndex<T> skipIndex) {
            this.table = table;

            foreach(var index in table.GetIndices()) {
                if (index.Name == skipIndex.Name) {
                    continue;
                }

                this.indices.Add(index);
            }
        }

        /// <summary>
        /// THREAD SAFE
        /// Collect the packets the passed record belongs to, except the packets
        /// which are under the skipped index. (See 'skipIndex' constructor parameter.)
        /// </summary>
        internal void TrackPackets(T record) {
            foreach(var index in this.indices) {
                var path = this.table.GetIndexPath(index, record);
                this.tracker.TryAdd(
                    $"{index.Name}\0{String.Join('\0', path)}",
                    Tuple.Create(index, path)
                );
            }
        }

        /// <summary>
        /// Returns an enumerator for the IEnumerable interface
        /// </summary>
        public IEnumerator<Packet<T>> GetEnumerator() {
            foreach (var item in this.tracker) {
                yield return new Packet<T>(this.table, item.Value.Item1, item.Value.Item2);
            }
        }

        /// <summary>
        /// Returns an enumerator for the IEnumerable interface
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }
    }
}
