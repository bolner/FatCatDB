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
using System.Threading.Tasks;
using NodaTime;
using ConController;

namespace FatCatDB.Test {
    [Controller(Name = "test", Description = "Test basic functionality")]
    public class TestController : ControllerBase {
        [EntryPoint(Name = "add", Description = "Create records in a table")]
        public static async Task AddRecords() {
            var db = new DbContext();
            var rnd = new Random(1234567890);
            var tr = db.Metrics.NewTransaction();

            var now = SystemClock.Instance.GetCurrentInstant().InZone(DateTimeZoneProviders.Tzdb.GetSystemDefault()).LocalDateTime;
            
            for (int i = 0; i < 2; i++) {
                var accountID = $"a{i + 10}";

                for (int j = 0; j < 5; j++) {
                    var campaignID = $"c{j + 10}a{i + 10}";

                    for (int k = 0; k < 10; k++) {
                        var adID = $"ad{k + 1000}c{j + 10}a{i + 10}";

                        for (int l = 0; l < 5; l++) {
                            var te = new MetricsRecord();

                            te.Date = new LocalDate(2020, 1, l + 1);
                            te.AccountID = accountID;
                            te.CampaignID = campaignID;
                            te.AdID = adID;
                            te.LastUpdated = new LocalDateTime(2020, 2, 2, 12, 21, 05);
                            te.Impressions = rnd.Next(1000, 100000);
                            te.Clicks = rnd.Next(100, 1000);
                            te.Conversions = rnd.Next(1, 100);
                            te.Revenue = rnd.NextDouble() * 100;
                            te.Cost = rnd.NextDouble() * 50;

                            tr.Add(te);
                        }
                    }
                }
            }

            tr.Commit();
        }

        [EntryPoint(Name = "query1", Description = "Query records and print them to STDOUT")]
        public static async Task QueryData1() {
            var db = new DbContext();
            db.Metrics.Query()
                .OrderByAsc(x => x.Date)
                .OrderByAsc(x => x.AccountID)
                .OrderByDesc(x => x.Conversions)
                .OrderByAsc(x => x.Revenue)
                .Print();
        }

        [EntryPoint(Name = "query2", Description = "Query records and print them to STDOUT")]
        public static async Task QueryData2() {
            var db = new DbContext();
            db.Metrics.Query()
                .Where(x => x.Date, "2020-01-02")
                .Where(x => x.AccountID, "a11")
                .OrderByAsc(x => x.CampaignID)
                .OrderByDesc(x => x.Cost)
                .Print();
        }

        [EntryPoint(Name = "query3", Description = "Query records and print them to STDOUT")]
        public static async Task QueryData3() {
            var db = new DbContext();
            db.Metrics.Query()
                .Where(x => x.Date, "2020-01-02")
                .Where(x => x.AccountID, "a11")
                .FlexFilter(x => x.Revenue > 50)
                .OrderByDesc(x => x.Cost)
                .Print();
        }

        [EntryPoint(Name = "stressTest", Description = "Create large amount of data")]
        public static async Task StressTest() {
            var db = new DbContext();
            var rnd = new Random(1234567890);
            var tr = db.Metrics.NewTransaction();

            for (int i = 0; i < 12; i++) {
                var accountID = $"a{i + 10}";

                for (int j = 0; j < 100; j++) {
                    var campaignID = $"c{j + 10}a{i + 10}";

                    for (int k = 0; k < 180; k++) {
                        var adID = $"ad{k + 1000}c{j + 10}a{i + 10}";

                        for (int l = 0; l < 30; l++) {
                            var te = new MetricsRecord();

                            te.Date = new LocalDate(2020, 1, l + 1);
                            te.AccountID = accountID;
                            te.CampaignID = campaignID;
                            te.AdID = adID;
                            te.LastUpdated = db.NowUTC;
                            te.Impressions = rnd.Next(1000, 100000);
                            te.Clicks = rnd.Next(100, 1000);
                            te.Conversions = rnd.Next(1, 100);
                            te.Revenue = rnd.NextDouble() * 100;
                            te.Cost = rnd.NextDouble() * 50;

                            tr.Add(te);
                        }
                    }
                }

                Console.Write($"Account {i} / 11");
                tr.Commit();
                Console.Write(" - Committed");
                System.GC.Collect();
                Console.WriteLine(" -");
            }
        }

        [EntryPoint(Name = "stressQuery", Description = "Query large amount of data")]
        public static async Task StressQuery() {
            var db = new DbContext();
            db.Metrics.Query()
                .OrderByAsc(x => x.Date)
                .OrderByAsc(x => x.AccountID)
                .GetExporter()
                .PrintToFile("var/query.tsv");
        }
    }
}
