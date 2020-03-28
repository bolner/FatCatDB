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
        private class UpdateTask {
            private Table<T> table;
            private Packet<T> packet;
            private PacketQuery<T> packetQuery;
            private Action<T> updater;
            private PacketCollector<T> packetCollector = null;

            internal UpdateTask(Packet<T> packet, PacketQuery<T> packetQuery, Action<T> updater,
                    PacketCollector<T> packetCollector = null) {
                
                this.table = packetQuery.Query.Table;
                this.packet = packet;
                this.packetQuery = packetQuery;
                this.updater = updater;
                this.packetCollector = packetCollector;
            }

            private void Update(List<T> records) {
                foreach(var record in records) {
                    if (this.packetCollector != null) {
                        this.packetCollector.TrackPackets(record);
                    }
                    
                    /*
                        Changing indexed fields is not allowed, because
                        that would require an additional read/write pass,
                        because then the record belongs to a different
                        packet. (It would be inefficient to do a second pass.)
                    */
                    var oldIndexPath = String.Join('\0', this.table.GetIndexPath(this.packetQuery.TableIndex, record));
                    var oldUnique = table.GetUnique(record);

                    this.updater(record);

                    var newIndexPath = String.Join('\0', this.table.GetIndexPath(this.packetQuery.TableIndex, record));
                    if (oldIndexPath != newIndexPath) {
                        throw new FatCatException($"An update query tried to change indexed fields in a record for table "
                            + $"'{table.Annotation.Name}'. Changing indexed fields inside OnUpdate functions is not allowed.");
                    }

                    /*
                        If the unique key has changed,
                        then remove the entry under the old key,
                        before setting the new.
                    */
                    var newUnique = table.GetUnique(record);
                    if (newUnique != oldUnique) {
                        packet.Remove(oldUnique);
                    }

                    packet.Set(newUnique, record);
                    continue;
                }
            }

            internal void Work() {
                using(Locking.GetMutex(packet.FullPath).Lock()) {
                    packet.Load();
                    packet.DeserializeDecompress();
                    var records = packet.GetFilteredRecords(this.packetQuery);

                    this.Update(records);
                    
                    packet.SerializeCompress();
                    packet.Save();
                }
            }

            internal async Task WorkAsync() {
                using(await Locking.GetMutex(packet.FullPath).LockAsync()) {
                    await packet.LoadAsync();
                    packet.DeserializeDecompress();
                    var records = packet.GetFilteredRecords(this.packetQuery);

                    this.Update(records);
                    
                    packet.SerializeCompress();
                    await packet.SaveAsync();
                }
            }
        }
    }
}
