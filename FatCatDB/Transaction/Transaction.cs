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
    public partial class Transaction<T> where T : class, new() {
        private Table<T> table;
        private List<T> records = new List<T>();
        private Dictionary<string, bool> remove = new Dictionary<string, bool>();
        private List<TableIndex<T>> indices = null;
        private int parallelism;
        private Exception exception = null;
        private Func<T, T, T> updateEventHandler = null;
        private UpdateQuery<T> updateQuery = null;
        private DeleteQuery<T> deleteQuery = null;
        private Action<T> updater = null;

        private Dictionary<string, PacketPlan> packetPlans = new Dictionary<string, PacketPlan>();

        /// <summary>
        /// Collects the records for a packet to be added or updated.
        /// Keeps track of the unique keys of the records.
        /// </summary>
        private class PacketPlan {
            private Table<T> table;
            internal Dictionary<string, T> Records { get; } = new Dictionary<string, T>();
            internal HashSet<string> Removed { get; } = new HashSet<string>();
            internal TableIndex<T> Index { get; }
            internal List<string> IndexPath { get; }
            internal string IndexPathStr { get; }

            internal PacketPlan(Table<T> table, TableIndex<T> index, List<string> indexPath, string indexPathStr) {
                this.table = table;
                this.Index = index;
                this.IndexPath = indexPath;
                this.IndexPathStr = indexPathStr;
            }

            internal void Add(T record) {
                var unique = table.GetUnique(record);

                Records[unique] = record;
                Removed.Remove(unique);
            }

            internal void Remove(T record) {
                var unique = table.GetUnique(record);

                this.Removed.Add(unique);
                this.Records.Remove(unique);
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
                    packetPlans[indexPathStr] = new PacketPlan(table, index, indexPath, indexPathStr);
                }

                packetPlans[indexPathStr].Add(record);
            }

            return this;
        }

        /// <summary>
        /// Removes a record by its unique key.
        /// </summary>
        public Transaction<T> Remove(T record) {
            foreach(var index in indices) {
                var indexPath = table.GetIndexPath(index, record);
                var indexPathStr = String.Join('\0', indexPath);
                
                if (!packetPlans.ContainsKey(indexPathStr)) {
                    packetPlans[indexPathStr] = new PacketPlan(table, index, indexPath, indexPathStr);
                }

                packetPlans[indexPathStr].Remove(record);
            }

            return this;
        }

        /// <summary>
        /// Save all changes onto the underlying device.
        /// </summary>
        /// <param name="garbageCollection">True = force garbage collection after the commit.</param>
        public void Commit(bool garbageCollection = false) {
            /*
                Delete query
            */
            if (this.deleteQuery != null) {
                var queryPlan = new QueryPlan<T>(this.deleteQuery.QueryBase);
                var packetCollector = new PacketCollector<T>(this.table, queryPlan.BestIndex);
                var cursor = new PacketCursor(queryPlan);
                var packetQueries = this.GetPacketQueries(this.deleteQuery.QueryBase);

                Parallel.ForEach(
                    cursor,
                    new ParallelOptions {MaxDegreeOfParallelism = this.parallelism},
                    packet => {
                        var task = new DeleteTask(packet, queryPlan.PacketQuery, packetCollector);
                        task.Work();
                    }
                );

                Parallel.ForEach(
                    packetCollector,
                    new ParallelOptions {MaxDegreeOfParallelism = this.parallelism},
                    packet => {
                        var task = new DeleteTask(packet, packetQueries[packet.IndexName]);
                        task.Work();
                    }
                );
            }

            /*
                Update query
            */
            if (this.updateQuery != null) {
                var queryPlan = new QueryPlan<T>(this.updateQuery.QueryBase);
                var packetCollector = new PacketCollector<T>(this.table, queryPlan.BestIndex);
                var cursor = new PacketCursor(queryPlan);
                var packetQueries = this.GetPacketQueries(this.updateQuery.QueryBase);

                Parallel.ForEach(
                    cursor,
                    new ParallelOptions {MaxDegreeOfParallelism = this.parallelism},
                    packet => {
                        var task = new UpdateTask(packet, queryPlan.PacketQuery, this.updater, packetCollector);
                        task.Work();
                    }
                );

                Parallel.ForEach(
                    packetCollector,
                    new ParallelOptions {MaxDegreeOfParallelism = this.parallelism},
                    packet => {
                        var task = new UpdateTask(packet, packetQueries[packet.IndexName], this.updater);
                        task.Work();
                    }
                );
            }

            /*
                Upsert or remove (specific records)
            */
            if (this.packetPlans.Count > 0) {
                Parallel.ForEach(
                    packetPlans,
                    new ParallelOptions {MaxDegreeOfParallelism = this.parallelism},
                    item => CommitThread(item.Value)
                );
            }
            
            records.Clear();
            remove.Clear();
            packetPlans.Clear();
            this.deleteQuery = null;
            this.updateQuery = null;

            if (this.exception != null) {
                this.exception = null;
                throw this.exception;
            }

            if (garbageCollection) {
                System.GC.Collect();
            }
        }

        /// <summary>
        /// Generates an associative array for quckly getting a PacketQuery
        /// for a specific index.
        /// </summary>
        private Dictionary<string, PacketQuery<T>> GetPacketQueries(QueryBase<T> queryBase) {
            var result = new Dictionary<string, PacketQuery<T>>();

            foreach(var index in this.table.GetIndices()) {
                result[index.Name] = new PacketQuery<T>(index, queryBase);
            }

            return result;
        }

        /// <summary>
        /// Save all changes onto the underlying device.
        /// </summary>
        /// <param name="garbageCollection">True = force garbage collection after the commit.</param>
        public async Task CommitAsync(bool garbageCollection = false) {
            /*
                Delete query
            */
            if (this.deleteQuery != null) {
                var queryPlan = new QueryPlan<T>(this.deleteQuery.QueryBase);
                var packetCollector = new PacketCollector<T>(this.table, queryPlan.BestIndex);
                var cursor = new PacketCursor(queryPlan);
                var packetQueries = this.GetPacketQueries(this.deleteQuery.QueryBase);

                await cursor.ParallelForEachAsync(
                    async packet => {
                        var task = new DeleteTask(packet, queryPlan.PacketQuery, packetCollector);
                        await task.WorkAsync();
                    },
                    maxDegreeOfParallelism: this.parallelism
                );

                await packetCollector.ParallelForEachAsync(
                    async packet => {
                        var task = new DeleteTask(packet, packetQueries[packet.IndexName]);
                        await task.WorkAsync();
                    },
                    maxDegreeOfParallelism: this.parallelism
                );
            }

            /*
                Update query
            */
            if (this.updateQuery != null) {
                var queryPlan = new QueryPlan<T>(this.updateQuery.QueryBase);
                var packetCollector = new PacketCollector<T>(this.table, queryPlan.BestIndex);
                var cursor = new PacketCursor(queryPlan);
                var packetQueries = this.GetPacketQueries(this.updateQuery.QueryBase);

                await cursor.ParallelForEachAsync(
                    async packet => {
                        var task = new UpdateTask(packet, queryPlan.PacketQuery, this.updater, packetCollector);
                        await task.WorkAsync();
                    },
                    maxDegreeOfParallelism: this.parallelism
                );

                await packetCollector.ParallelForEachAsync(
                    async packet => {
                        var task = new UpdateTask(packet, packetQueries[packet.IndexName], this.updater);
                        await task.WorkAsync();
                    },
                    maxDegreeOfParallelism: this.parallelism
                );
            }

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
            this.deleteQuery = null;
            this.updateQuery = null;

            if (this.exception != null) {
                this.exception = null;
                throw this.exception;
            }

            if (garbageCollection) {
                System.GC.Collect();
            }
        }

        private void UpdatePacketFromPlan(PacketPlan plan, Packet<T> packet) {
            foreach(var item in plan.Records) {
                if (this.updateEventHandler != null) {
                    var old = packet.Get(item.Key);

                    if (old != null) {
                        /*
                            Changing indexed fields is not allowed, because
                            that would require an additional read/write pass,
                            because then the record belongs to a different
                            packet. (It would be inefficient to do a second pass.)
                        */
                        var updatedRecord = updateEventHandler(old, item.Value);
                        if (updatedRecord == null) {
                            /*
                                If the event handler returns NULL, then it
                                wants to avoid the update.
                            */
                            continue;
                        }

                        var newIndexPath = String.Join('\0', table.GetIndexPath(plan.Index, updatedRecord));
                        if (newIndexPath != plan.IndexPathStr) {
                            throw new FatCatException($"An update event handler has changed indexed fields in "
                                + $"a record for table '{table.Annotation.Name}'. Changing indexed fields inside OnUpdate functions is not allowed.");
                        }

                        /*
                            If the unique key has changed,
                            then remove the entry under the old key,
                            before setting the new.
                        */
                        var newUnique = table.GetUnique(updatedRecord);
                        if (newUnique != item.Key) {
                            packet.Remove(item.Key);
                        }

                        packet.Set(newUnique, updatedRecord);
                        continue;
                    }
                }

                packet.Set(item.Key, item.Value);
            }

            foreach(var key in plan.Removed) {
                packet.Remove(key);
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

                    this.UpdatePacketFromPlan(packetPlan, packet);

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

                    this.UpdatePacketFromPlan(packetPlan, packet);

                    packet.SerializeCompress();
                    await packet.SaveAsync();
                }
            } catch (Exception ex) {
                this.exception = ex;
            }
        }

        /// <summary>
        /// Set up an event handler for update events.
        /// </summary>
        public void OnUpdate(Func<T, T, T> eventHandler) {
            this.updateEventHandler = eventHandler;
        }

        /// <summary>
        /// Allows to specify records to be updated for this table during the commit phase.
        /// Use the OnUpdate() method to specify what should happen with the updated
        /// records.
        /// </summary>
        public UpdateQuery<T> Update(Action<T> updater) {
            this.updateQuery = new UpdateQuery<T>(this.table, this);
            this.updater = updater;

            return this.updateQuery;
        }

        /// <summary>
        /// Allows to specify records to be deleted for this table during the commit phase.
        /// </summary>
        public DeleteQuery<T> Delete() {
            this.deleteQuery = new DeleteQuery<T>(this.table, this);

            return this.deleteQuery;
        }

        /// <summary>
        /// Removes all records from this table during the commit phase.
        /// </summary>
        public void Truncate() {
            // ?
        }
    }
}
