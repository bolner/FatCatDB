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
        private Bookmark.BookmarkFragment bookmarkFragment;
        private bool bookmarkApplied = false;
        private FilenameEncoder filenameEncoder = new FilenameEncoder();
        private string pathPrefix;

        /// <summary>
        /// This keeps track of the last record fetched,
        /// to be used in a bookmark.
        /// </summary>
        private T lastRecordFetched = null;

        /// <summary>
        /// The active payloads. FILO queue.
        /// </summary>
        private Queue<PacketLoaderTask> payloads = new Queue<PacketLoaderTask>();

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
            var bookmark = this.queryPlan.Query.Bookmark;

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

            /*
                Check if the bookmark contains a fragment for this engine.
            */
            if (bookmark != null) {
                this.bookmarkFragment = null;

                foreach(var fragment in bookmark.Fragments) {
                    if (fragment.TableName == this.table.Annotation.Name
                        && fragment.IndexName == this.queryPlan.BestIndex.Name) {
                        
                        this.bookmarkFragment = fragment;
                        break;
                    }
                }

                if (this.bookmarkFragment == null) {
                    throw new FatCatException($"Invalid bookmark. Please always use the bookmarks in the same "
                        + "queries they were created for. (2)");
                }
            }
        }

        /// <summary>
        /// Returns the next packet or null if the query ended.
        /// It neither loads nor deserializes the packet.
        /// </summary>
        internal Packet<T> FetchNextPacket() {
            if (this.noMorePackets) {
                return null;
            }

            do {
                if (executionPath.Count >= queryPlan.BestIndex.PropertyIndices.Count) {
                    /*
                        We've found an item. It will be returned for this call.
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
                    IComparable afterValue = null;

                    if (this.bookmarkFragment != null && !this.bookmarkApplied) {
                        string columnName = this.table.ColumnNames[propIndex];
                        if (!this.bookmarkFragment.Path.ContainsKey(columnName)) {
                            throw new FatCatException($"Invalid bookmark. Please always use the bookmarks in the same "
                                + "queries they were created for. (3)");
                        }

                        afterValue = table.ConvertStringToValue(propIndex, this.bookmarkFragment.Path[columnName]);
                    }
                    
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
                                files = this.ListFilesInFolder(currentPath, true, isLastLevel, propIndex, afterValue);
                            } else {
                                // Descending
                                files = this.ListFilesInFolder(currentPath, false, isLastLevel, propIndex, afterValue);
                            }
                        } else {
                            // Ascending by default
                            files = this.ListFilesInFolder(currentPath, true, isLastLevel, propIndex, afterValue);
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
        /// packet. Applies limit.
        /// </summary>
        /// <param name="isQueryEnded">True = The limit was reached, False = Otherwise</param>
        /// <returns>The next item or null when we finished processing the current packet.</returns>
        private T RecordTraversing(out bool isQueryEnded) {
            isQueryEnded = false;

            if (this.activeRecords != null) {
                if (this.activeRecords.Length > this.activeRecordIndex) {
                    if (this.limit > 0) {
                        if (this.absoluteRecordIndex >= this.limit) {
                            // Reached the limit
                            isQueryEnded = true;
                            return null;
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
                    this.lastRecordFetched = next;
                    return next;
                }

                this.CreateThreads(isAsync: false);

                if (this.payloads.Count < 1) {
                    this.activeRecords = null;
                    this.activeRecordIndex = 0;
                    return null;
                }

                var payload = this.payloads.Dequeue();
                payload.Wait();

                if (payload.Exception != null) {
                    // Wait for all before throwing the exception
                    foreach(var item in this.payloads) {
                        payload.Wait();
                    }

                    throw payload.Exception;
                }

                this.activeRecords = payload.Packet.GetRecords();
                this.activeRecordIndex = FindActiveRecordIndex();
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
                    this.lastRecordFetched = next;
                    return next;
                }

                this.CreateThreads(isAsync: true);

                if (this.payloads.Count < 1) {
                    this.activeRecords = null;
                    this.activeRecordIndex = 0;
                    return null;
                }

                var payload = this.payloads.Dequeue();
                await payload.WaitAsync();

                if (payload.Exception != null) {
                    // Wait for all before throwing the exception
                    foreach(var item in this.payloads) {
                        await payload.WaitAsync();
                    }

                    throw payload.Exception;
                }

                this.activeRecords = payload.Packet.GetRecords();
                this.activeRecordIndex = FindActiveRecordIndex();
            } while (true);
        }

        /// <summary>
        /// Uses the bookmark to find the next record.
        /// </summary>
        /// <returns>The index of the next record, or "activeRecords.Length" if we
        ///     should jump to the next packet.</returns>
        private long FindActiveRecordIndex() {
            if (this.bookmarkFragment == null || this.bookmarkApplied) {
                return 0;
            }

            /*
                Deactivate the bookmark (if any), as we've already filled
                    up the execution path for the first time and the first
                    packet will already be filtered.
            */
            this.bookmarkApplied = true;
            
            var values = bookmarkFragment.GetPropertyValues<T>(this.table);
            bool equal;

            for(int i = 0; i < this.activeRecords.Length; i++) {
                equal = true;

                foreach(var value in values) {
                    var recordValue = (IComparable)table.Properties[value.Key].GetValue(this.activeRecords[i]);
                    
                    if (recordValue.CompareTo(value.Value) != 0) {
                        equal = false;
                        break;
                    }
                }

                if (equal) {
                    /*
                        This is intentionally returning an invalid index
                        "activeRecords.Length", when the found record
                        was the last in the packet.
                        The record traversing code can handle that index.
                    */
                    return i + 1;
                }
            }

            throw new FatCatException("Failed to apply bookmark. The bookmarked record has been removed since the query "
                + "for which the bookmark was generated.");
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
                this.payloads.Enqueue(payload);
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
        /// Return a list of decoded base- file and directory names in a folder.
        /// </summary>
        /// <param name="folder">Target folder</param>
        /// <param name="asc">Sort the result ascending=true or descending=false</param>
        /// <param name="isLastLevel">On the last level it lists files, otherwise directories</param>
        /// <param name="propertyIndex">Identifies the column and its type</param>
        /// <param name="afterValue">If not NULL, then return values which come after this value.</param>
        private string[] ListFilesInFolder(string folder, bool asc, bool isLastLevel, int propertyIndex, IComparable afterValue) {
            IEnumerable<Tuple<IComparable, string>> files;
            IOrderedEnumerable<Tuple<IComparable, string>> orderedFiles;

            try {
                if (isLastLevel) {
                    /*
                        Files
                    */
                    files = Directory.EnumerateFiles(folder, "*.tsv.gz", SearchOption.TopDirectoryOnly)
                        .Select(x => {
                            var fileBase = filenameEncoder.Decode(Path.GetFileName(x).Replace(".tsv.gz", ""));
                            var value = table.ConvertStringToValue(propertyIndex, fileBase);
                            return Tuple.Create(value, fileBase);
                        });
                } else {
                    /*
                        Directories
                    */
                    files = Directory.EnumerateDirectories(folder, "*", SearchOption.TopDirectoryOnly)
                        .Select(x => {
                            var fileBase = filenameEncoder.Decode(Path.GetFileName(x));
                            var value = table.ConvertStringToValue(propertyIndex, fileBase);
                            return Tuple.Create(value, fileBase);
                        });
                }
            } catch (Exception) {
                return new string[] { };
            }

            if (afterValue != null) {
                if (asc) {
                    files = files.Where(x => x.Item1.CompareTo(afterValue) >= 0);
                } else {
                    files = files.Where(x => x.Item1.CompareTo(afterValue) <= 0);
                }
            }

            if (asc) {
                orderedFiles = files.OrderBy(x => x.Item1);
            } else {
                orderedFiles = files.OrderByDescending(x => x.Item1);
            }

            return orderedFiles.Select(x => x.Item2).ToArray();
        }

        /// <summary>
        /// Returns the last record fetched by the query engine.
        /// The response can be used for creating a bookmark.
        /// </summary>
        internal T GetLastRecordFetched() {
            return this.lastRecordFetched;
        }
    }
}
