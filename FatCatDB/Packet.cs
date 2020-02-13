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
using System.IO.Compression;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;
using LinearTsvParser;

namespace FatCatDB {
    /// <summary>
    /// The in-memory representation of a tsv.gz file.
    /// </summary>
    /// <typeparam name="T">Annotated class of a database table record</typeparam>
    internal class Packet<T> where T : class, new() {
        private Table<T> table;
        internal string IndexName { get; }
        internal string TableName { get; }
        internal string DirName { get; }
        internal string FileName { get; }
        internal string FullPath { get; }
        internal string[] Header { get; }
        private Dictionary<string, T> data = new Dictionary<string, T>();
        private List<T> lines = new List<T>();
        private bool isDurabilityEnabled;

        /*
            Memory storage for raw data.
        */
        private byte[] rawCompressedData = null;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="table">DB table</param>
        /// <param name="index">The index which contains the packet in its folder structure</param>
        /// <param name="indexPath">Actual values of the indexed columns</param>
        internal Packet(Table<T> table, TableIndex<T> index, List<string> indexPath) {
            this.table = table;
            this.TableName = table.Annotation.Name;
            this.IndexName = index.Name;
            var path = new List<string>(indexPath);
            this.isDurabilityEnabled = this.table.DbContext.Configuration.IsDurabilityEnabled;

            if (table.DbContext.Configuration.DatabasePath != null) {
                path.Insert(0, Path.Join(
                    table.DbContext.Configuration.DatabasePath,
                    this.TableName, IndexName
                ));
            } else {
                path.Insert(0, Path.Join("var", "data", this.TableName, IndexName));
            }

            DirName = Path.Join(path.SkipLast(1).ToArray());
            FileName = $"{path.Last()}.tsv.gz";
            FullPath = Path.Join(DirName, FileName);
            this.Header = table.ColumnNames;
        }

        /// <summary>
        /// Update an existing record (by unique key) or
        /// add a new one if doesn't exist yet.
        /// This method won't check if the record actually
        /// belongs to the index path of the packet. That
        /// check has to be done earlier separately to
        /// ensure a better performance.
        /// </summary>
        /// <param name="unique">The unique key of the record</param>
        /// <param name="record">A new record to add or to update an existing with its content.</param>
        internal void Set(string unique, T record) {
            this.data[unique] = record;
        }

        /// <summary>
        /// Returns the record for its unique key or
        /// NULL if not found.
        /// </summary>
        internal T Get(string unique) {
            T record = null;
            this.data.TryGetValue(unique, out record);

            return record;
        }

        /// <summary>
        /// Remove a record by its unique key.
        /// </summary>
        internal void Remove(string unique) {
            this.data.Remove(unique);
        }

        /// <summary>
        /// Load the data if exists
        /// </summary>
        internal void Load() {
            /*
                Read file if exists
            */
            if (File.Exists(FullPath)) {
                var buffer = new byte[1024 * 1024];
                using var ms = new MemoryStream();
                using var file = File.OpenRead(FullPath);
                int read = 0;

                do {
                    read = file.Read(buffer, 0, buffer.Length);
                    // Don't use async write for the memory stream
                    ms.Write(buffer, 0, read);
                } while (read > 0);

                ms.Flush();
                rawCompressedData = ms.ToArray();
            }
        }

        /// <summary>
        /// Load the data if exists
        /// </summary>
        internal async Task LoadAsync() {
            /*
                Read file if exists
            */
            if (File.Exists(FullPath)) {
                var buffer = new byte[1024 * 1024];
                using var ms = new MemoryStream();
                using var file = File.OpenRead(FullPath);
                int read = 0;

                do {
                    read = await file.ReadAsync(buffer, 0, buffer.Length);
                    // Don't use async write for the memory stream
                    ms.Write(buffer, 0, read);
                } while (read > 0);

                ms.Flush();
                rawCompressedData = ms.ToArray();
            }
        }

        /// <summary>
        /// Decompress the raw data and desirialize the records.
        /// </summary>
        /// <param name="queryPlan">Optional for queries. Filters the recrods if passed.</param>
        internal void DeserializeDecompress(QueryPlan<T> queryPlan = null) {
            if (rawCompressedData == null) {
                return;
            }

            List<string> header = null;
            TsvMapping<T> tsvMapping = null;
            using var ms = new MemoryStream(rawCompressedData);
            using var gzip = new GZipStream(ms, CompressionMode.Decompress);
            using var tsv = new TsvReader(gzip);
            int lineCount = 0;
            Nullable<int> tsvColumnIndex = null;
            bool isFirstLine = true;
            bool notMatching = false;
            
            while(!tsv.EndOfStream) {
                var line = tsv.ReadLine();

                if (isFirstLine) {
                    // Use its own header for compatibility.
                    header = line;
                    isFirstLine = false;
                    tsvMapping = new TsvMapping<T>(table, header);
                    continue;
                }

                if (header.Count != line.Count) {
                    throw new Exception($"Header length and column count mismatch in file '{FullPath}' at line {lineCount + 1}.");
                }

                /*
                    Filtering by 'Where' expressions
                */
                if (queryPlan != null) {
                    notMatching = false;

                    foreach(var filter in queryPlan.FreeIndexFilters) {
                        tsvColumnIndex = tsvMapping.FromRecordToTsv[filter.Key];

                        if (tsvColumnIndex == null || line[(int)tsvColumnIndex] != filter.Value) {
                            notMatching = true;
                            break;
                        }
                    }

                    if (notMatching) {
                        continue;
                    }
                }

                var record = new T();
                table.LoadFromTSVLine(tsvMapping, record, line);

                /*
                    Filtering by flex expressions
                */
                if (queryPlan != null) {
                    notMatching = false;

                    foreach(var exp in queryPlan.Query.FlexFilters) {
                        if (!exp(record)) {
                            notMatching = true;
                            break;
                        }
                    }

                    if (notMatching) {
                        continue;
                    }
                }

                string unique = table.GetUnique(record);
                this.data[unique] = record;
                this.lines.Add(record);

                lineCount++;
            }

            /*
                Sort records inside a packet
            */
            if (queryPlan != null) {
                if (queryPlan.FreeSorting.Count > 0) {
                    var props = table.Properties.ToArray();

                    this.lines.Sort((x, y) => {
                        foreach(var directive in queryPlan.FreeSorting) {
                            int dir = directive.Item2 == SortingDirection.Ascending ? 1 : -1;
                            var valueX = props[directive.Item1].GetValue(x);
                            var valueY = props[directive.Item1].GetValue(y);

                            if (valueX == null && valueY == null) {
                                continue;
                            }

                            if (valueX == null) {
                                return -dir;
                            }

                            if (valueY == null) {
                                return dir;
                            }

                            return ((IComparable)valueX).CompareTo((IComparable)valueY) * dir;
                        }

                        return 0;
                    });
                }
            }
        }

        /// <summary>
        /// Converts the records to TSV representation and
        /// compresses the results.
        /// </summary>
        internal void SerializeCompress() {
            using var ms = new MemoryStream();

            using(var gzip = new GZipStream(ms, CompressionMode.Compress))
            using(var tsv = new TsvWriter(gzip)) {
                tsv.WriteLine(Header);
                var values = new string[Header.Length];

                foreach(T record in this.data.Values) {
                    table.GetStringValues(record, values);
                    tsv.WriteLine(values);
                }
            }
            rawCompressedData = ms.ToArray();
        }

        /// <summary>
        /// Writes the packet out to the path defined by the index.
        /// Guaratees that the data is physically written onto
        /// the underlying device.
        /// It first writes to a temp file and then moves it to its
        /// real path, to increase durability.
        /// </summary>
        internal void Save() {
            string tmpPath;

            if (this.isDurabilityEnabled) {
                tmpPath = $"{FullPath}.tmp";
            } else {
                tmpPath = FullPath;
            }

            try {
                Directory.CreateDirectory(DirName);
            } catch (Exception ex) {
                throw new FatCatException($"Unable to create directory '{DirName}'. Please check the permissions of the parent folders.", ex);
            }
            
            try {
                using var outfile = File.Create(tmpPath);
                outfile.Write(rawCompressedData);

                // To guarantee that the data hits the physical drive
                outfile.Flush(true);
            } catch (Exception ex) {
                throw new FatCatException($"Unable to write to file '{tmpPath}'. Please check the permissions "
                    + "of its folder or the available disk space.", ex);
            }

            if (this.isDurabilityEnabled) {
                try {
                    File.Delete(FullPath);
                } catch (Exception ex) {
                    /*
                        There are no exceptions in case of a missing file,
                        so the problem is something else here.
                    */
                    throw new FatCatException($"Unable to delete file '{FullPath}'. Please move the file '{tmpPath}' to "
                        + "its place manually, then resume the application.", ex);
                }

                try {
                    File.Move(tmpPath, FullPath);
                } catch (Exception ex) {
                    throw new FatCatException($"Failed to move file from '{tmpPath}' to '{FullPath}'."
                        + " Please do this operation manually, then resume the application.", ex);
                }
            }
        }

        /// <summary>
        /// Writes the packet out to the path defined by the index.
        /// Guaratees that the data is physically written onto
        /// the underlying device.
        /// It first writes to a temp file and then moves it to its
        /// real path, to increase durability.
        /// </summary>
        internal async Task SaveAsync() {
            string tmpPath;

            if (this.isDurabilityEnabled) {
                tmpPath = $"{FullPath}.tmp";
            } else {
                tmpPath = FullPath;
            }

            /*
                Unfortunately there are no async methods for creating
                a directory or a file entry in .NET. The reason is
                probably that the Windows API doesn't support
                callbacks for these calls, so an implementation would
                always require a thread pool on Windows.
            */
            try {
                Directory.CreateDirectory(DirName);
            } catch (Exception ex) {
                throw new FatCatException($"Unable to create directory '{DirName}'. Please check the permissions of the parent folders.", ex);
            }

            try {
                using FileStream outfile = File.Create(tmpPath);
                await outfile.WriteAsync(rawCompressedData);

                // To guarantee that the data hits the physical drive
                outfile.Flush(true);
            } catch (Exception ex) {
                throw new FatCatException($"Unable to write to file '{tmpPath}'. Please check the permissions "
                    + "of its folder or the available disk space.", ex);
            }

            if (this.isDurabilityEnabled) {
                try {
                    File.Delete(FullPath);
                } catch (Exception ex) {
                    /*
                        There are no exceptions in case of a missing file,
                        so the problem is something else here.
                    */
                    throw new FatCatException($"Unable to delete file '{FullPath}'. Please move the file '{tmpPath}' to "
                        + "its place manually, then resume the application.", ex);
                }
                
                try {
                    File.Move(tmpPath, FullPath);
                } catch (Exception ex) {
                    throw new FatCatException($"Failed to move file from '{tmpPath}' to '{FullPath}'."
                        + " Please do this operation manually, then resume the application.", ex);
                }
            }
        }

        /// <summary>
        /// Returns the records in the packet.
        /// (Filtering might have been applied.)
        /// </summary>
        internal T[] GetRecords() {
            return this.lines.ToArray();
        }
    }
}
