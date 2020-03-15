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
using System.Threading;
using System.Threading.Tasks;

namespace FatCatDB {
    internal partial class QueryEngine<T> {
        /// <summary>
        /// Contains both the info required for starting the work,
        /// and the results, after it's done.
        /// </summary>
        private class PacketLoaderTask {
            private ManualResetEvent TaskCompleted { get; } = new ManualResetEvent(false);
            internal Packet<T> Packet { get; }
            private QueryPlan<T> queryPlan;
            private Exception exception = null;
            internal Exception Exception { get { return this.exception; } }
            private bool isAsync;
            private Task task;
            
            internal PacketLoaderTask(Packet<T> packet, QueryPlan<T> queryPlan, bool isAsync) {
                this.Packet = packet;
                this.queryPlan = queryPlan;
                this.isAsync = isAsync;

                if (isAsync) {
                    this.task = WorkerAsync();
                } else {
                    ThreadPool.QueueUserWorkItem(this.WorkerCallback);
                }
            }

            private void WorkerCallback(object state) {
                try {
                    using (Locking.GetMutex(this.Packet.FullPath).Lock()) {
                        this.Packet.Load();
                    }

                    this.Packet.DeserializeDecompress();
                } catch (Exception ex) {
                    this.exception = ex;
                }

                this.TaskCompleted.Set();
            }

            private async Task WorkerAsync() {
                try {
                    using (Locking.GetMutex(this.Packet.FullPath).Lock()) {
                        await this.Packet.LoadAsync();
                    }

                    this.Packet.DeserializeDecompress();
                } catch (Exception ex) {
                    this.exception = ex;
                }

                this.TaskCompleted.Set();
            }

            /// <summary>
            /// Synchronous wait for completion
            /// </summary>
            internal void Wait() {
                this.TaskCompleted.WaitOne();
            }

            /// <summary>
            /// Async wait for completion
            /// </summary>
            internal async Task WaitAsync() {
                await this.task;
            }
        }
    }
}
