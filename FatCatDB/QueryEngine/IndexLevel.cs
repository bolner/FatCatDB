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

namespace FatCatDB {
    internal partial class QueryEngine<T> {
        /// <summary>
        /// A level in the execution stack.
        /// List of files in a folder, that can
        /// be iterated on.
        /// </summary>
        private class IndexLevel {
            private Int64 position = 0;
            internal string[] Files { get; } = null;

            internal IndexLevel(string[] files) {
                this.Files = files;
            }

            internal IndexLevel(string file) {
                this.Files = new string[] { file };
            }

            internal bool HasMore {
                get {
                    return position < Files.Length - 1;
                }
            }

            internal string Next() {
                if (position < Files.Length - 1) {
                    position++;
                }
                
                return Files[position];
            }

            internal string Current() {
                return Files[position];
            }
        }
    }
}
