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
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FatCatDB {
    public partial class Transaction<T> where T : class, new() {
        private class PacketCursor : IEnumerable<Packet<T>> {
            private QueryPlan<T> queryPlan;
            private QueryEngine<T> queryEngine;

            internal PacketCursor(QueryPlan<T> queryPlan) {
                this.queryPlan = queryPlan;
                this.queryEngine = new QueryEngine<T>(queryPlan);
            }

            /// <summary>
            /// Returns an enumerator for the IEnumerable interface
            /// </summary>
            public IEnumerator<Packet<T>> GetEnumerator() {
                Packet<T> next;

                while ((next = queryEngine.FetchNextPacket()) != null) {
                    yield return next;
                }
            }

            /// <summary>
            /// Returns an enumerator for the IEnumerable interface
            /// </summary>
            IEnumerator IEnumerable.GetEnumerator() {
                return GetEnumerator();
            }
        }
    }
}
