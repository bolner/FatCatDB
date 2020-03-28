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
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FatCatDB {
    public partial class Transaction<T> where T : class, new() {
        private class DeleteTask {
            private Table<T> table;
            private Packet<T> packet;
            private PacketQuery<T> packetQuery;
            private PacketCollector<T> packetCollector = null;

            internal DeleteTask(Packet<T> packet, PacketQuery<T> packetQuery,
                    PacketCollector<T> packetCollector = null) {
                
                this.table = packetQuery.Query.Table;
                this.packet = packet;
                this.packetQuery = packetQuery;
                this.packetCollector = packetCollector;
            }

            private void Delete(List<T> records) {
                foreach(var record in records) {
                    this.packet.Remove(table.GetUnique(record));
                }
            }

            internal void Work() {
                using(Locking.GetMutex(packet.FullPath).Lock()) {
                    packet.Load();
                    packet.DeserializeDecompress();
                    var records = packet.GetFilteredRecords(this.packetQuery);

                    this.Delete(records);
                    
                    if (this.packet.IsEmpty()) {
                        packet.DeletePacketFile();
                    } else {
                        packet.SerializeCompress();
                        packet.Save();
                    }
                }
            }

            internal async Task WorkAsync() {
                using(await Locking.GetMutex(packet.FullPath).LockAsync()) {
                    await packet.LoadAsync();
                    packet.DeserializeDecompress();
                    var records = packet.GetFilteredRecords(this.packetQuery);

                    this.Delete(records);
                    
                    if (this.packet.IsEmpty()) {
                        packet.DeletePacketFile();
                    } else {
                        packet.SerializeCompress();
                        await packet.SaveAsync();
                    }
                }
            }
        }
    }
}
