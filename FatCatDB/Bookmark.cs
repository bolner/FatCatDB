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
using System.Text;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FatCatDB {
    /// <summary>
    /// Bookmarks can be used for paging queries with "limit" directives.
    /// A bookmark identifies the last record in a query, and can
    /// be used to continue the same query after that record.
    /// </summary>
    internal class Bookmark {
        [JsonProperty]
        internal List<BookmarkFragment> Fragments { get; } = new List<BookmarkFragment>();

        /// <summary>
        /// A bookmark has multiple fragments in case of a composite
        /// query, created by one or more JOIN directives.
        /// </summary>
        internal class BookmarkFragment {
            [JsonProperty]
            private string tableName;
            internal string TableName { get { return tableName; } }

            [JsonProperty]
            private string indexName;
            internal string IndexName { get { return indexName; } }

            [JsonProperty]
            internal Dictionary<string, string> Path { get; } = new Dictionary<string, string>();

            [JsonConstructor]
            internal BookmarkFragment() {

            }

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="tableName">Table name</param>
            /// <param name="indexName">Index name</param>
            /// <param name="values">The index/unique path values for the last record</param>
            internal BookmarkFragment(string tableName, string indexName, JObject values) {
                this.tableName = tableName;
                this.indexName = indexName;

                foreach(var item in values) {
                    this.Path[item.Key] = item.Value.ToString();
                }
            }

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="tableName">Table name</param>
            /// <param name="indexName">Index name</param>
            /// <param name="values">The index/unique path values for the last record</param>
            internal BookmarkFragment(string tableName, string indexName, Dictionary<string, string> values) {
                this.tableName = tableName;
                this.indexName = indexName;
                this.Path = new Dictionary<string, string>(values);
            }

            /// <summary>
            /// Returns the path filter values in their original type,
            /// indexed by the property indices.
            /// </summary>
            /// <returns>An array of the length of table properties.</returns>
            internal Dictionary<int, IComparable> GetPropertyValues<T>(Table<T> table) where T : class, new() {
                Dictionary<int, IComparable> result = new Dictionary<int, IComparable>();
                
                foreach(var item in this.Path) {
                    int propindex = table.ColumnNameToPropertyIndex(item.Key);
                    if (propindex < 0) {
                        throw new FatCatException($"Invalid bookmark. Please always use the bookmarks in the same "
                            + "queries they were created for. (1)");
                    }

                    result[propindex] = table.ConvertStringToValue(propindex, item.Value);
                }

                return result;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        internal Bookmark() {

        }

        /// <summary>
        /// Factory method.
        /// </summary>
        /// <param name="bookmarkText">The Base64-encoded JSON representation of the Bookmark.</param>
        internal static Bookmark FromString(string bookmarkText) {
            try {
                string decoded = Encoding.UTF8.GetString(
                    Convert.FromBase64String(bookmarkText)
                );

                return JsonConvert.DeserializeObject<Bookmark>(decoded);
            } catch (Exception ex) {
                throw new FatCatException("Invalid bookmark format. Please make sure that the string is "
                    + $"not modified before using it in a query.", ex);
            }
        }

        /// <summary>
        /// Adds the next level of bookmark fragment.
        /// </summary>
        /// <param name="tableName">The name of the table on which the query was executed.</param>
        /// <param name="indexName">An index in the table.</param>
        /// <param name="recordPath">You can get this by Table.GetFullRecordPath(...)</param>
        internal void AddFragment(string tableName, string indexName, Dictionary<string, string> recordPath) {
            this.Fragments.Add(new BookmarkFragment(tableName, indexName, recordPath));
        }

        /// <summary>
        /// Returns the string representation of the bookmark
        /// </summary>
        public override string ToString() {
            return Convert.ToBase64String(
                Encoding.UTF8.GetBytes(
                    JsonConvert.SerializeObject(this)
                )
            );
        }
    }
}
