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
        private List<BookmarkFragment> fragments = new List<BookmarkFragment>();

        /// <summary>
        /// A bookmark has multiple fragments in case of a composite
        /// query, created by one or more JOIN directives.
        /// </summary>
        private class BookmarkFragment {
            private string indexName;
            private Dictionary<string, string> values = new Dictionary<string, string>();

            internal BookmarkFragment(string indexName, JObject values) {
                this.indexName = indexName;

                foreach(var item in values) {
                    this.values[item.Key] = item.Value.ToString();
                }
            }

            internal BookmarkFragment(string indexName, Dictionary<string, string> values) {
                this.indexName = indexName;
                this.values = new Dictionary<string, string>(values);
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        internal Bookmark() {

        }

        /// <summary>
        /// Construcor
        /// </summary>
        /// <param name="bookmarkText">The Base64-encoded JSON representation of the Bookmark.</param>
        internal Bookmark(string bookmarkText) {
            try {
                string decoded = Encoding.UTF8.GetString(
                    Convert.FromBase64String(bookmarkText)
                );

                JObject json = JsonConvert.DeserializeObject<JObject>(decoded);

                foreach(var fragment in json) {
                    if (fragment.Value is JObject) {
                        this.fragments.Add(new BookmarkFragment(fragment.Key, (JObject)fragment.Value));
                    } else {
                        throw new Exception("The bookmark has to contain a JSON object of objects, encoded in Base64.");
                    }
                }
            } catch (Exception ex) {
                throw new FatCatException("Invalid bookmark format. Please make sure that the string is "
                    + "not modified before using it in a query.", ex);
            }
        }

        /// <summary>
        /// Adds the next level of bookmark fragment.
        /// </summary>
        /// <param name="indexName">An index in the table. (The table is determined by the order of the fragments.)</param>
        /// <param name="recordPath">You can get this by Table.GetFullRecordPath(...)</param>
        internal void AddFragment(string indexName, Dictionary<string, string> recordPath) {
            this.fragments.Add(new BookmarkFragment(indexName, recordPath));
        }
    }
}
