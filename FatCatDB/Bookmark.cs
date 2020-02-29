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
            internal string TableName { get; }

            [JsonProperty]
            internal string IndexName { get; }

            [JsonProperty]
            internal Dictionary<string, string> Path { get; } = new Dictionary<string, string>();

            internal BookmarkFragment(string tableName, string indexName, JObject values) {
                this.TableName = tableName;
                this.IndexName = indexName;

                foreach(var item in values) {
                    this.Path[item.Key] = item.Value.ToString();
                }
            }

            internal BookmarkFragment(string tableName, string indexName, Dictionary<string, string> values) {
                this.TableName = tableName;
                this.IndexName = indexName;
                this.Path = new Dictionary<string, string>(values);
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
                    + "not modified before using it in a query.", ex);
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

        public override string ToString() {
            return JsonConvert.SerializeObject(this);
        }
    }
}
