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
using System.Threading.Tasks;
using LinearTsvParser;

namespace FatCatDB {
    /// <summary>
    /// For exporting the data in different formats
    /// </summary>
    /// <typeparam name="T">An annotated database table record class</typeparam>
    public class Exporter<T> where T : class, new() {
        private Table<T> table;
        private Cursor<T> cursor;

        /// <summary>
        /// Constructor
        /// </summary>
        public Exporter(Table<T> table, Cursor<T> cursor) {
            this.table = table;
            this.cursor = cursor;
        }

        /// <summary>
        /// Print the query result to the standard output
        /// </summary>
        public void Print() {
            using var tsvWriter = new TsvWriter(Console.Out);

            PrintToTsvWriter(tsvWriter);
        }

        /// <summary>
        /// Print the query result to the standard output
        /// </summary>
        public async Task PrintAsync() {
            using var tsvWriter = new TsvWriter(Console.Out);

            await PrintToTsvWriterAsync(tsvWriter);
        }

        /// <summary>
        /// Output the query result into a TsvWriter instance
        /// </summary>
        public void PrintToTsvWriter(TsvWriter output) {
            output.WriteLine(table.ColumnNames);
            var values = new string[table.ColumnNames.Length];
            
            foreach(var item in cursor) {
                table.GetStringValues(item, values);
                output.WriteLine(values);
            }
        }

        /// <summary>
        /// Output the query result into a TsvWriter instance
        /// </summary>
        public async Task PrintToTsvWriterAsync(TsvWriter output) {
            await output.WriteLineAsync(table.ColumnNames);
            var values = new string[table.ColumnNames.Length];
            T item;
            
            while ((item = await cursor.FetchNextAsync()) != null) {
                table.GetStringValues(item, values);
                await output.WriteLineAsync(values);
            }
        }

        /// <summary>
        /// Output the query result into a Stream instance
        /// </summary>
        public void PrintToStream(Stream stream) {
            using var tsvWriter = new TsvWriter(stream);

            PrintToTsvWriter(tsvWriter);
        }

        /// <summary>
        /// Output the query result into a Stream instance
        /// </summary>
        public async Task PrintToStreamAsync(Stream stream) {
            using var tsvWriter = new TsvWriter(stream);

            await PrintToTsvWriterAsync(tsvWriter);
        }

        /// <summary>
        /// Write the query response into a file.
        /// </summary>
        public void PrintToFile(string path) {
            using var output = File.OpenWrite(path);
            using var tsvWriter = new TsvWriter(output);

            PrintToTsvWriter(tsvWriter);
        }

        /// <summary>
        /// Write the query response into a file.
        /// </summary>
        public async Task PrintToFileAsync(string path) {
            using var output = File.OpenWrite(path);
            using var tsvWriter = new TsvWriter(output);

            await PrintToTsvWriterAsync(tsvWriter);
        }

        /// <summary>
        /// Returns the string representation of the bookmark.
        /// Using the bookmark one can continue a limited query
        /// at the point it stopped.
        /// </summary>
        public string GetBookmark() {
            return this.cursor.GetBookmark();
        }
    }
}
