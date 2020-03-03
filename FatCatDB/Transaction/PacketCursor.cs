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
