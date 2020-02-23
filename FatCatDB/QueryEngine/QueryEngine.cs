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
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace FatCatDB
{
    /// <summary>
    /// Executes a query
    /// </summary>
    /// <typeparam name="T">An annotated class of a database table record</typeparam>
    internal partial class QueryEngine<T> where T: class, new() {
        private Table<T> table;
        private QueryPlan<T> queryPlan;
        private Stack<IndexLevel> executionPath = new Stack<IndexLevel>();
        private int paralellism;
        private bool noMorePackets = false;
        private long limit;
        private long offset;
        private FilenameEncoder filenameEncoder = new FilenameEncoder();

        /// <summary>
        /// This keeps track of the last record fetched,
        /// to be used in a bookmark.
        /// </summary>
        private T lastRecordFetched = null;

        /// <summary>
        /// This will be the next item to serve to the user.
        /// </summary>
        private long TaskSequenceFirst = 0;

        /// <summary>
        /// The next task gets "TaskSequenceMax + 1" as ID
        /// </summary>
        private long TaskSequenceNext = 0;

        /// <summary>
        /// The active payloads. The old ones get removed.
        /// </summary>
        /// <typeparam name="long">Sequence number</typeparam>
        /// <typeparam name="TaskPayload">A thread works on it, and stores its results in it.</typeparam>
        private Dictionary<long, PacketLoaderTask> payloads = new Dictionary<long, PacketLoaderTask>();

        /// <summary>
        /// These are the records of the packet that
        /// is fetched just now.
        /// </summary>
        private T[] activeRecords = null;
        private long activeRecordIndex = 0;

        /// <summary>
        /// This tracks the number of total records returned.
        /// Used for limit/offset.
        /// </summary>
        private long absoluteRecordIndex = 0;

        private List<Func<T, bool>> filterExpressions;
        private Dictionary<int, string> indexFilters;
        private List<Tuple<int, SortingDirection>> sorting;

        /// <summary>
        /// Constructor
        /// </summary>
        internal QueryEngine(QueryPlan<T> queryPlan) {
            this.table = queryPlan.Table;
            this.queryPlan = queryPlan;
            this.paralellism = table.DbContext.Configuration.QueryParallelism;
            this.filterExpressions = queryPlan.Query.FlexFilters;
            this.sorting = queryPlan.Query.Sorting;
            this.indexFilters = queryPlan.Query.IndexFilters;
            this.limit = this.queryPlan.Query.QueryLimit;
            this.offset = this.queryPlan.Query.Offset;
        }

        /// <summary>
        /// Returns the next packet or null if the query ended.
        /// It neither loads nor deserializes the packet.
        /// </summary>
        internal Packet<T> FetchNextPacket() {
            if (this.noMorePackets) {
                return null;
            }

            string pathPrefix;

            if (this.table.DbContext.Configuration.DatabasePath != null) {
                pathPrefix = Path.Join(
                    this.table.DbContext.Configuration.DatabasePath,
                    table.Annotation.Name, queryPlan.BestIndex.Name
                );
            } else {
                pathPrefix = Path.Join(
                    "var", "data",
                    table.Annotation.Name, queryPlan.BestIndex.Name
                );
            }
            
            do {
                if (executionPath.Count >= queryPlan.BestIndex.PropertyIndices.Count) {
                    /*
                        We've found an item. This will be returned for this call.
                    */
                    var packet = new Packet<T>(
                        this.table, queryPlan.BestIndex,
                        executionPath.Select(x => x.Current()).Reverse().ToList()
                    );

                    /*
                        Move the execution path to the next item, to continue from
                        there at the next call.
                        If there are none, then set the "query ended" property.
                    */
                    this.Traverse();
                    if (this.executionPath.Count == 0) {
                        this.noMorePackets = true;
                    }

                    return packet;
                }
                else {
                    /*
                        Fill in the missing levels in the execution path
                    */
                    int propIndex = queryPlan.BestIndex.PropertyIndices[executionPath.Count];
                    bool isLastLevel = executionPath.Count == queryPlan.BestIndex.PropertyIndices.Count - 1;

                    if (this.indexFilters.ContainsKey(propIndex)) {
                        executionPath.Push(new IndexLevel(this.indexFilters[propIndex]));
                    }
                    else {
                        string currentPath = Path.Join(
                            pathPrefix,
                            Path.Join(executionPath.Select(x => x.Current()).Reverse().ToArray())
                        );

                        string[] files;

                        if (queryPlan.SortingAssoc.ContainsKey(propIndex)) {
                            if (queryPlan.SortingAssoc[propIndex] == SortingDirection.Ascending) {
                                // Ascending
                                files = this.ListFilesInFolder(currentPath, false, isLastLevel, propIndex);
                            } else {
                                // Descending
                                files = this.ListFilesInFolder(currentPath, false, isLastLevel, propIndex);
                            }
                        } else {
                            files = this.ListFilesInFolder(currentPath, true, isLastLevel, propIndex);
                        }

                        if (files.Length < 1) {
                            // If the directory is empty, then turn back
                            this.Traverse();
                            continue;
                        }

                        executionPath.Push(new IndexLevel(files));
                    }
                }
            } while (executionPath.Count > 0);

            this.noMorePackets = true;
            return null;
        }

        /// <summary>
        /// Moves through the records in the currently opened
        /// packet. Applies limit and offset.
        /// </summary>
        /// <param name="isQueryEnded">True = The limit was reached, False = Otherwise</param>
        /// <returns>The next item or null when we finished processing the current packet.</returns>
        private T RecordTraversing(out bool isQueryEnded) {
            isQueryEnded = false;

            if (this.activeRecords != null) {
                if (this.activeRecords.Length > this.activeRecordIndex) {
                    if (this.limit > 0) {
                        if (this.absoluteRecordIndex >= this.offset + this.limit) {
                            // Reached the limit
                            isQueryEnded = true;
                            return null;
                        }
                    }

                    if (this.absoluteRecordIndex < this.offset) {
                        // Both are "number of items"
                        long remainsFromPacket = this.activeRecords.Length - this.activeRecordIndex - 1;
                        long remainsFromOffset = this.absoluteRecordIndex - this.offset;

                        if (remainsFromPacket <= remainsFromOffset) {
                            // Jump whole packet
                            this.absoluteRecordIndex += remainsFromPacket;
                            return null;
                        } else {
                            // Jump inside the packet
                            this.absoluteRecordIndex += remainsFromOffset;
                            this.activeRecordIndex += remainsFromOffset;
                        }
                    }

                    T next = this.activeRecords[this.activeRecordIndex];
                    this.activeRecordIndex++;
                    this.absoluteRecordIndex++;

                    return next;
                }
            }

            return null;
        }

        /// <summary>
        /// Returns the next record or null if the query ended.
        /// </summary>
        internal T FetchNextRecord() {
            T next;
            bool isQueryEnded;

            do {
                next = this.RecordTraversing(out isQueryEnded);
                if (isQueryEnded) {
                    this.activeRecords = null;
                    this.activeRecordIndex = 0;
                    return null;
                }

                if (next != null) {
                    return next;
                }

                this.CreateThreads(isAsync: false);

                if (this.payloads.Count < 1) {
                    this.activeRecords = null;
                    this.activeRecordIndex = 0;
                    return null;
                }

                var payload = this.payloads[this.TaskSequenceFirst];
                payload.Wait();

                if (payload.Exception != null) {
                    // Wait for all before throwing the exception
                    foreach(var item in this.payloads) {
                        payload.Wait();
                    }

                    throw payload.Exception;
                }

                this.payloads.Remove(this.TaskSequenceFirst);
                this.TaskSequenceFirst++;
                this.activeRecords = payload.Packet.GetRecords();
                this.activeRecordIndex = 0;
            } while (true);
        }

        /// <summary>
        /// Returns the next record or null if the query ended.
        /// </summary>
        internal async Task<T> FetchNextRecordAsync() {
            T next;
            bool isQueryEnded;

            do {
                next = this.RecordTraversing(out isQueryEnded);
                if (isQueryEnded) {
                    this.activeRecords = null;
                    this.activeRecordIndex = 0;
                    return null;
                }

                if (next != null) {
                    return next;
                }

                this.CreateThreads(isAsync: true);

                if (this.payloads.Count < 1) {
                    this.activeRecords = null;
                    this.activeRecordIndex = 0;
                    return null;
                }

                var payload = this.payloads[this.TaskSequenceFirst];
                await payload.WaitAsync();

                if (payload.Exception != null) {
                    // Wait for all before throwing the exception
                    foreach(var item in this.payloads) {
                        await payload.WaitAsync();
                    }

                    throw payload.Exception;
                }

                this.payloads.Remove(this.TaskSequenceFirst);
                this.TaskSequenceFirst++;
                this.activeRecords = payload.Packet.GetRecords();
                this.activeRecordIndex = 0;
            } while (true);
        }

        /// <summary>
        /// Creates new worker threads until the required
        /// amount is reached.
        /// </summary>
        private void CreateThreads(bool isAsync) {
            if (noMorePackets) {
                return;
            }

            for (int i = 0; i < this.paralellism - this.payloads.Count; i++) {
                var packet = this.FetchNextPacket();
                if (packet == null) {
                    return;
                }

                var payload = new PacketLoaderTask(packet, this.queryPlan, isAsync);
                this.payloads[this.TaskSequenceNext] = payload;
                this.TaskSequenceNext++;
            }
        }

        /// <summary>
        /// Move backwards in the execution stack until
        /// we find a level that has more items to
        /// iterate on.
        /// </summary>
        private void Traverse() {
            while (executionPath.Count > 0) {
                if (executionPath.Peek().HasMore) {
                    executionPath.Peek().Next();
                    return;
                }

                // Discard
                executionPath.Pop();
            }
        }

        /// <summary>
        /// Return a list of files or directories in a folder.
        /// </summary>
        /// <param name="folder">Target folder</param>
        /// <param name="asc">Sort the result ascending=true or descending=false</param>
        /// <param name="isLastLevel">On the last level it lists files, otherwise directories</param>
        /// <param name="propertyIndex">Identifies the column and its type</param>
        private string[] ListFilesInFolder(string folder, bool asc, bool isLastLevel, int propertyIndex) {
            string[] files;

            try {
                if (isLastLevel) {
                    /*
                        Files
                    */
                    if (asc) {
                        files = Directory.EnumerateFiles(folder, "*.tsv.gz", SearchOption.TopDirectoryOnly)
                            .Select(x => {
                                var fileBase = Path.GetFileName(x).Replace(".tsv.gz", "");
                                var value = table.ConvertStringToValue(propertyIndex, filenameEncoder.Decode(fileBase));
                                return Tuple.Create(value, fileBase);
                            })
                            .OrderByDescending(x => x.Item1)
                            .Select(x => x.Item2)
                            .ToArray();
                    } else {
                        files = Directory.EnumerateFiles(folder, "*.tsv.gz", SearchOption.TopDirectoryOnly)
                            .Select(x => {
                                var fileBase = Path.GetFileName(x).Replace(".tsv.gz", "");
                                var value = table.ConvertStringToValue(propertyIndex, filenameEncoder.Decode(fileBase));
                                return Tuple.Create(value, fileBase);
                            })
                            .OrderBy(x => x.Item1)
                            .Select(x => x.Item2)
                            .ToArray();
                    }
                } else {
                    /*
                        Directories
                    */
                    if (asc) {
                        files = Directory.EnumerateDirectories(folder, "*", SearchOption.TopDirectoryOnly)
                            .Select(x => {
                                var fileBase = Path.GetFileName(x);
                                var value = table.ConvertStringToValue(propertyIndex, filenameEncoder.Decode(fileBase));
                                return Tuple.Create(value, fileBase);
                            })
                            .OrderByDescending(x => x.Item1)
                            .Select(x => x.Item2)
                            .ToArray();
                    } else {
                        files = Directory.EnumerateDirectories(folder, "*", SearchOption.TopDirectoryOnly)
                            .Select(x => {
                                var fileBase = Path.GetFileName(x);
                                var value = table.ConvertStringToValue(propertyIndex, filenameEncoder.Decode(fileBase));
                                return Tuple.Create(value, fileBase);
                            })
                            .OrderBy(x => x.Item1)
                            .Select(x => x.Item2)
                            .ToArray();
                    }
                }
            } catch (Exception) {
                files = new string[] { };
            }

            return files;
        }
    }
}
