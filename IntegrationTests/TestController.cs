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
    [Controller(Name = "test", Description = "Test basic functionality")]
    public class TestController : ControllerBase {
        [EntryPoint(Name = "add", Description = "Create records in a table")]
        public static async Task AddRecords() {
            var db = new DbContext();
            var rnd = new Random(1234567890);
            var tr = db.Metrics.NewTransaction();

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
                            te.Created = new LocalDateTime(2020, 2, 2, 12, 21, 05);
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

        [EntryPoint(Name = "query1", Description = "Query all, sort on 4 fields.")]
        public static async Task QueryData1() {
            var db = new DbContext();
            db.Metrics.Query()
                .OrderByAsc(x => x.Date)
                .OrderByAsc(x => x.AccountID)
                .OrderByDesc(x => x.Conversions)
                .OrderByAsc(x => x.Revenue)
                .Print();
        }

        [EntryPoint(Name = "query2", Description = "Query by date and account. Sort on 2 fields.")]
        public static async Task QueryData2() {
            var db = new DbContext();
            db.Metrics.Query()
                .Where(x => x.Date, "2020-01-02")
                .Where(x => x.AccountID, "a11")
                .OrderByAsc(x => x.CampaignID)
                .OrderByDesc(x => x.Cost)
                .Print();
        }

        [EntryPoint(Name = "query3", Description = "Query by date and account. Use flexfilter. Sort by cost.")]
        public static async Task QueryData3() {
            var db = new DbContext();
            await db.Metrics.Query()
                .Where(x => x.Date, "2020-01-02")
                .Where(x => x.AccountID, "a11")
                .FlexFilter(x => x.Revenue > 50)
                .OrderByDesc(x => x.Cost)
                .PrintAsync();
        }

        [EntryPoint(Name = "query4", Description = "Filter by date and account.")]
        public static async Task QueryData4() {
            var db = new DbContext();
            db.Metrics.Query()
                .Where(x => x.Date, "2020-01-02")
                .Where(x => x.AccountID, "a11")
                .OrderByDesc(x => x.AdID)
                .Print();
        }

        [EntryPoint(Name = "update", Description = "Update some of the records")]
        public static async Task UpdateRecords1() {
            var db = new DbContext();
            var transaction = db.Metrics.NewTransaction();

            transaction.OnUpdate((oldRecord, newRecord) => {
                newRecord.Created = oldRecord.Created;
                newRecord.Clicks = oldRecord.Clicks + 1;
                
                return newRecord;
            });

            var rnd = new Random(1234567890);

            var items = db.Metrics.Query()
                .Where(x => x.Date, "2020-01-02")
                .Where(x => x.AccountID, "a11")
                .OrderByAsc(x => x.AdID)
                .Limit(1000)
                .GetCursor();

            foreach(var item in items) {
                item.Date = new LocalDate(2020, 1, 2);
                item.LastUpdated = new LocalDateTime(2020, 2, 6, 11, 08, 46);
                item.Created = db.NowUTC;
                item.Impressions = rnd.Next(1000, 100000);
                item.Clicks = rnd.Next(100, 1000);
                item.Conversions = rnd.Next(1, 100);
                item.Revenue = rnd.NextDouble() * 100;
                item.Cost = rnd.NextDouble() * 50;

                transaction.Add(item);
            }

            await transaction.CommitAsync();
        }

        [EntryPoint(Name = "queryPlan1", Description = "Output the query plan for a complex query")]
        public static async Task QueryPlan1() {
            var db = new DbContext();
            var plan = db.Metrics.Query()
                .Where(x => x.AccountID, "a11")
                .OrderByAsc(x => x.Date)
                .OrderByAsc(x => x.Cost)
                .FlexFilter(x => x.Impressions > x.Clicks && x.Revenue > 0)
                .Limit(100, 10)
                .GetQueryPlan();
            
            Console.Write(plan);
        }

        [EntryPoint(Name = "queryPlan2", Description = "Output the query plan for a complex query")]
        public static async Task QueryPlan2() {
            var db = new DbContext();
            var plan = db.Metrics.Query()
                .Where(x => x.Date, "2020-01-02")
                .OrderByAsc(x => x.AccountID)
                .OrderByAsc(x => x.Cost)
                .FlexFilter(x => x.Impressions > x.Clicks && x.Revenue > 0)
                .GetQueryPlan();
            
            Console.Write(plan);
        }
    }
}
