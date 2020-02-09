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
using System.Threading.Tasks;
using System.Collections.Generic;
using Dasync.Collections;

namespace FatCatDB {
    /// <summary>
    /// A transaction object that can be used for creating,
    /// modifying and deleting records from a table.
    /// </summary>
    /// <typeparam name="T">Annotated class of a database record table</typeparam>
    public class Transaction<T> where T : class, new() {
        private Table<T> table;
        private List<T> records = new List<T>();
        private Dictionary<string, bool> remove = new Dictionary<string, bool>();
        private List<TableIndex<T>> indices = null;
        private int parallelism;
        private Exception exception = null;

        private Dictionary<string, PacketPlan> packetPlans = new Dictionary<string, PacketPlan>();

        /// <summary>
        /// Collects the records for a packet to be added or updated
        /// </summary>
        private class PacketPlan {
            public List<T> Records { get; }
            public TableIndex<T> Index { get; }
            public List<string> IndexPath { get; }

            public PacketPlan(List<T> records, TableIndex<T> index, List<string> indexPath) {
                this.Records = records;
                this.Index = index;
                this.IndexPath = indexPath;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        internal Transaction(Table<T> table) {
            this.table = table;
            indices = table.GetIndices();
            this.parallelism = table.DbContext.Configuration.TransactionParallelism;
        }

        /// <summary>
        /// Inserts or updates a record.
        /// Checks whether the record exists already by its unique key,
        /// if yes, then an update happens.
        /// </summary>
        public Transaction<T> Add(T record) {
            records.Add(record);

            foreach(var index in indices) {
                var indexPath = table.GetIndexPath(index, record);
                var indexPathStr = String.Join('\0', indexPath);
                
                if (!packetPlans.ContainsKey(indexPathStr)) {
                    packetPlans[indexPathStr] = new PacketPlan(new List<T>(), index, indexPath);
                }

                packetPlans[indexPathStr].Records.Add(record);
            }

            return this;
        }

        /// <summary>
        /// Removes a record by its unique key.
        /// </summary>
        public Transaction<T> Remove(T record) {
            remove[table.GetUnique(record)] = true;
            return this;
        }

        /// <summary>
        /// Save all changes onto the underlying device.
        /// </summary>
        /// <param name="garbageCollection">True = force garbage collection after the commit.</param>
        public void Commit(bool garbageCollection = false) {
            /*
                Insert, update or remove (per packet)
            */
            Parallel.ForEach(
                packetPlans,
                new ParallelOptions {MaxDegreeOfParallelism = this.parallelism},
                item => CommitThread(item.Value)
            );

            records.Clear();
            remove.Clear();
            packetPlans.Clear();

            if (this.exception != null) {
                this.exception = null;
                throw this.exception;
            }

            if (garbageCollection) {
                System.GC.Collect();
            }
        }

        /// <summary>
        /// Save all changes onto the underlying device.
        /// </summary>
        /// <param name="garbageCollection">True = force garbage collection after the commit.</param>
        public async Task CommitAsync(bool garbageCollection = false) {
            /*
                Insert, update or remove (per packet)
            */
            await packetPlans.ParallelForEachAsync(
                item => CommitThreadAsync(item.Value),
                maxDegreeOfParallelism: this.parallelism
            );

            records.Clear();
            remove.Clear();
            packetPlans.Clear();

            if (this.exception != null) {
                this.exception = null;
                throw this.exception;
            }

            if (garbageCollection) {
                System.GC.Collect();
            }
        }

        /// <summary>
        /// Entry point for a worker thread
        /// </summary>
        /// <param name="packetPlan">Payload</param>
        private void CommitThread(PacketPlan packetPlan) {
            if (this.exception != null) {
                return;
            }

            try {
                var packet = new Packet<T>(this.table, packetPlan.Index, packetPlan.IndexPath);
            
                using (Locking.GetMutex(packet.FullPath).Lock()) {
                    packet.Load();
                    packet.DeserializeDecompress();

                    foreach(var record in packetPlan.Records) {
                        var unique = table.GetUnique(record);

                        if (this.remove.ContainsKey(unique)) {
                            packet.Remove(unique);
                        } else {
                            packet.Set(record);
                        }
                    }

                    packet.SerializeCompress();
                    packet.Save();
                }
            } catch (Exception ex) {
                this.exception = ex;
            }
        }

        /// <summary>
        /// Entry point for a worker thread
        /// </summary>
        /// <param name="packetPlan">Payload</param>
        private async Task CommitThreadAsync(PacketPlan packetPlan) {
            if (this.exception != null) {
                return;
            }

            try {
                var packet = new Packet<T>(this.table, packetPlan.Index, packetPlan.IndexPath);

                using (await Locking.GetMutex(packet.FullPath).LockAsync()) {
                    await packet.LoadAsync();
                    packet.DeserializeDecompress();

                    foreach(var record in packetPlan.Records) {
                        var unique = table.GetUnique(record);

                        if (this.remove.ContainsKey(unique)) {
                            packet.Remove(unique);
                        } else {
                            packet.Set(record);
                        }
                    }

                    packet.SerializeCompress();
                    await packet.SaveAsync();
                }
            } catch (Exception ex) {
                this.exception = ex;
            }
        }
    }
}
