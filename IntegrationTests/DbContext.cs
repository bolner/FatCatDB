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
using NodaTime;
using NodaTime.Text;

namespace FatCatDB.Test {
    internal class DbContext : DbContextBase {
        public Table<MetricsRecord> Metrics { get; } = new Table<MetricsRecord>();
        
        private LocalDateTimePattern pattern = LocalDateTimePattern.CreateWithInvariantCulture(
            "yyyy-MM-dd HH:mm:ss"
        );

        protected override void OnConfiguring (TypeConverterSetup typeConverterSetup, Configurator configurator) {
            typeConverterSetup
                .RegisterTypeConverter<LocalDateTime, string>((x) => {
                    return x.ToString("yyyy-MM-dd HH:mm:ss", null);
                })
                .RegisterTypeConverter<string, LocalDateTime>((x) => {
                    return pattern.Parse(x).Value;
                });
            
            configurator
                .SetTransactionParallelism(1)
                .SetQueryParallelism(1)
                .EnableDurability(false);
        }
    }
}
