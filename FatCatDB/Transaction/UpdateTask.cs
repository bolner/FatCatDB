using System;
using System.Threading.Tasks;

namespace FatCatDB {
    public partial class Transaction<T> where T : class, new() {
        private class UpdateTask {
            Table<T> table;
            Packet<T> packet;
            QueryPlan<T> queryPlan;
            Action<T> updater;

            internal UpdateTask(Packet<T> packet, QueryPlan<T> queryPlan, Action<T> updater) {
                this.table = queryPlan.Table;
                this.packet = packet;
                this.queryPlan = queryPlan;
                this.updater = updater;
            }

            private void Update(T[] records) {
                foreach(var record in records) {
                    /*
                        Changing indexed fields is not allowed, because
                        that would require an additional read/write pass,
                        because then the record belongs to a different
                        packet. (It would be inefficient to do a second pass.)
                    */
                    var oldIndexPath = String.Join('\0', this.table.GetIndexPath(this.queryPlan.BestIndex, record));
                    var oldUnique = table.GetUnique(record);

                    this.updater(record);

                    var newIndexPath = String.Join('\0', this.table.GetIndexPath(this.queryPlan.BestIndex, record));
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
                    packet.DeserializeDecompress(queryPlan);
                    var records = packet.GetRecords();

                    // TODO: records are a subset, filtered load cannot be saved back
                    this.Update(records);
                    
                    packet.SerializeCompress();
                    packet.Save();
                }
            }

            internal async Task WorkAsync() {
                using(await Locking.GetMutex(packet.FullPath).LockAsync()) {
                    await packet.LoadAsync();
                    packet.DeserializeDecompress(queryPlan);
                    var records = packet.GetRecords();

                    // TODO: records are a subset, filtered load cannot be saved back
                    this.Update(records);
                    
                    packet.SerializeCompress();
                    await packet.SaveAsync();
                }
            }
        }
    }
}
