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
using FatCatDB.Annotation;
using NodaTime;

namespace FatCatDB.Test {
    [Table(Name = "metrics", Unique = "ad_id, date", NullValue = "")]
    [TableIndex(Name = "account_date", Columns = "account_id, date")]
    [TableIndex(Name = "date_account", Columns = "date, account_id")]
    internal class MetricsRecord {
        [Column(Name = "date")]
        public LocalDate? Date { get; set; }

        [Column(Name = "account_id")]
        public string AccountID { get; set; }

        [Column(Name = "campaign_id")]
        public string CampaignID { get; set; }

        [Column(Name = "ad_id")]
        public string AdID { get; set; }

        [Column(Name = "last_updated")]
        public LocalDateTime? LastUpdated { get; set; }

        [Column(Name = "created")]
        public LocalDateTime? Created { get; set; }

        [Column(Name = "impressions")]
        public long? Impressions { get; set; }

        [Column(Name = "clicks")]
        public long? Clicks { get; set; }

        [Column(Name = "conversion")]
        public long? Conversions { get; set; }

        [Column(Name = "revenue")]
        public decimal? Revenue { get; set; }

        [Column(Name = "cost")]
        public decimal? Cost { get; set; }
    }
}
