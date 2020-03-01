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
using System.Linq;
using System.Threading.Tasks;
using NodaTime;
using ConController;

namespace FatCatDB.Test {
    [Controller(Name = "bookmarkTest", Description = "Test bookmarks")]
    public class BoomarkTestController : ControllerBase {
        [EntryPoint(Name = "add", Description = "Create records in a table")]
        public static async Task AddRecords() {
            var db = new DbContext();
            var rnd = new Random(1234567890);
            var tr = db.Metrics.NewTransaction();
            int AdID = 10000;

            for (int i = 0; i < 2; i++) {
                var accountID = $"a{i + 10}";

                for (int j = 0; j < 5; j++) {
                    var campaignID = $"c{j + 10}a{i + 10}";

                    for (int k = 0; k < 10; k++) {
                        AdID++;

                        for (int l = 0; l < 5; l++) {
                            var te = new MetricsRecord();

                            te.Date = new LocalDate(2020, 1, l + 1);
                            te.AccountID = accountID;
                            te.CampaignID = campaignID;
                            te.AdID = AdID.ToString();
                            te.Created = new LocalDateTime(2020, 2, 2, 12, 21, 05);
                            te.LastUpdated = new LocalDateTime(2020, 2, 2, 12, 21, 05);
                            te.Impressions = rnd.Next(1000, 100000);
                            te.Clicks = rnd.Next(100, 1000);
                            te.Conversions = rnd.Next(1, 100);
                            te.Revenue = (decimal?)(rnd.NextDouble() * 100);
                            te.Cost = (decimal?)(rnd.NextDouble() * 50);

                            tr.Add(te);
                        }
                    }
                }
            }

            tr.Commit();
        }

        private static Query<MetricsRecord> GetQuery(DbContext db) {
            return db.Metrics.Query()
                .OrderByAsc(x => x.AdID)
                .FlexFilter(x => x.Impressions > 5000);
        }

        [EntryPoint(Name = "queryFull", Description = "Query the full data set")]
        public static async Task QueryFull() {
            var db = new DbContext();

            var exporter = GetQuery(db)
                .GetExporter();

            await exporter.PrintAsync();
            Console.Write($"\n\nThe bookmark:\n\n{exporter.GetBookmark()}\n");
        }

        [EntryPoint(Name = "queryPage1", Description = "Query the first page and the bookmark")]
        public static async Task Query1() {
            var db = new DbContext();

            var exporter = GetQuery(db)
                .Limit(12)
                .GetExporter();

            await exporter.PrintAsync();
            Console.Write($"\n\nThe bookmark:\n\n{exporter.GetBookmark()}\n");
        }

        [EntryPoint(Name = "queryPage2", Description = "Query the second page and the bookmark")]
        public static async Task Query2() {
            var db = new DbContext();

            var exporter = GetQuery(db)
                .Limit(12)
                .AfterBookmark("eyJGcmFnbWVudHMiOlt7InRhYmxlTmFtZSI6Im1ldHJpY3MiLCJpbmRleE5hbWUiOiJhY2NvdW50X2RhdGUiLCJQYXRoIjp7ImFjY291bnRfaWQiOiJhMTAiLCJkYXRlIjoiMjAyMC0wMS0wMSIsImFkX2lkIjoiMTAwMTIifX1dfQ==")
                .GetExporter();

            await exporter.PrintAsync();
            Console.Write($"\n\nThe bookmark:\n\n{exporter.GetBookmark()}\n");
        }
    }
}
