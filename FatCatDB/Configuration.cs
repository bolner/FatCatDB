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

namespace FatCatDB {
    /// <summary>
    /// Helper class for generating the configuration
    /// </summary>
    public class Configurator {
        /// <summary>
        /// The number of threads working on adding and modifying data
        /// </summary>
        private int transactionParallelism = 8;

        /// <summary>
        /// The number of treads working on query execution and deserialization
        /// </summary>
        private int queryParallelism = 8;

        /// <summary>
        /// The location of the database files on a drive
        /// </summary>
        private string databasePath = null;

        /// <summary>
        /// If the durability function is enabled, then the data modification
        /// operations are slower, but the user is fully protected from
        /// data loss. Note that the likelyhood of data loss is already very low
        /// when using multiple indexes, since the data is stored redundantly,
        /// therefore this option is disabled by default.
        /// </summary>
        private bool enableDurability = false;

        /// <summary>
        /// The number of threads working on adding and modifying data
        /// </summary>
        public Configurator SetTransactionParallelism(int value) {
            transactionParallelism = value;
            return this;
        }

        /// <summary>
        /// The number of treads working on query execution and deserialization
        /// </summary>
        public Configurator SetQueryParallelism(int value) {
            queryParallelism = value;
            return this;
        }

        /// <summary>
        /// The location of the database files on a drive
        /// </summary>
        public Configurator SetDatabasePath(string value) {
            databasePath = value;
            return this;
        }

        /// <summary>
        /// If the durability function is enabled, then the data modification
        /// operations are slower, but the user is fully protected from
        /// data loss. Note that the likelyhood of data loss is already very low
        /// when using multiple indexes, since the data is stored redundantly,
        /// therefore this option is disabled by default.
        /// </summary>
        public Configurator EnableDurability(bool enable) {
            this.enableDurability = enable;
            return this;
        }

        /// <summary>
        /// The number of threads working on adding and modifying data
        /// </summary>
        internal int GetTransactionParallelism() {
            return transactionParallelism;
        }

        /// <summary>
        /// The number of treads working on query execution and deserialization
        /// </summary>
        internal int GetQueryParallelism() {
            return queryParallelism;
        }

        /// <summary>
        /// The location of the database files on a drive
        /// </summary>
        internal string GetDatabasePath() {
            return databasePath;
        }

        /// <summary>
        /// If the durability function is enabled, then the data modification
        /// operations are slower, but the user is fully protected from
        /// data loss. Note that the likelyhood of data loss is already very low
        /// when using multiple indexes, since the data is stored redundantly,
        /// therefore this option is disabled by default.
        /// </summary>
        internal bool IsDurabilityEnabled() {
            return this.enableDurability;
        }
    }

    /// <summary>
    /// Configuration
    /// </summary>
    internal class Configuration {
        /// <summary>
        /// The number of threads working on adding and modifying data
        /// </summary>
        public int TransactionParallelism { get; }

        /// <summary>
        /// The number of treads working on query execution and deserialization
        /// </summary>
        public int QueryParallelism { get; }

        /// <summary>
        /// The location of the database files on a drive
        /// </summary>
        public string DatabasePath { get; }

        /// <summary>
        /// If the durability function is enabled, then the data modification
        /// operations are slower, but the user is fully protected from
        /// data loss. Note that the likelyhood of data loss is already very low
        /// when using multiple indexes, since the data is stored redundantly,
        /// therefore this option is disabled by default.
        /// </summary>
        public bool IsDurabilityEnabled { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="configurator"></param>
        public Configuration(Configurator configurator) {
            TransactionParallelism = configurator.GetTransactionParallelism();
            QueryParallelism = configurator.GetQueryParallelism();
            DatabasePath = configurator.GetDatabasePath();
            IsDurabilityEnabled = configurator.IsDurabilityEnabled();

            /*
                Change the thread pool size if not changed yet
            */
            int paralellism = TransactionParallelism + QueryParallelism;
            int workerThreads, completionPortThreads;
            
            ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);
            if (workerThreads != paralellism || completionPortThreads != paralellism) {
                ThreadPool.SetMaxThreads(paralellism, paralellism);
            }
        }
    }
}
