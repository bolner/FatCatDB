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
using System.Linq.Expressions;

namespace FatCatDB {
    /// <summary>
    /// A query for deleting records
    /// </summary>
    /// <typeparam name="T">An annotated class of a database table record</typeparam>
    public class DeleteQuery<T> where T : class, new() {
        internal QueryBase<T> QueryBase { get; }
        private Transaction<T> transaction;

        /// <summary>
        /// Constructor
        /// </summary>
        public DeleteQuery(Table<T> table, Transaction<T> transaction) {
            this.QueryBase = new QueryBase<T>(table);
            this.transaction = transaction;
        }

        /// <summary>
        /// Fast filtering, using indices. If you would like to filter using
        /// arbitrary expressions, then use the 'FlexFilter' method instead.
        /// The 'FlexFilter' is slower than the 'Where', because that's not
        /// using indices.
        /// </summary>
        /// <param name="property">Filter by this column of the table</param>
        /// <param name="value">Exact match with this value</param>
        public DeleteQuery<T> Where(Expression<Func<T, object>> property, IComparable value) {
            this.QueryBase.Where(property, value);
            
            return this;
        }

        /// <summary>
        /// Generic filtering. In contrary to the 'Where' method, the 'FlexFilter' method
        /// doesn't use indices, so its query time is linear, but it can handle
        /// arbitrary filter expressions. Please use it in combination with the
        /// 'Where' method for optimal performace.
        /// </summary>
        /// <param name="filterExpression">An arbitrary expression, involving the columns of the table.</param>
        public DeleteQuery<T> FlexFilter(Func<T, bool> filterExpression) {
            this.QueryBase.FlexFilter(filterExpression);

            return this;
        }

        /// <summary>
        /// Add a sorting directive for a specific field.
        /// Multiple field sorting is supported.
        /// </summary>
        public DeleteQuery<T> OrderByAsc(Expression<Func<T, object>> property) {
            this.QueryBase.OrderByAsc(property);

            return this;
        }

        /// <summary>
        /// Add a sorting directive for a specific field.
        /// Multiple field sorting is supported.
        /// </summary>
        public DeleteQuery<T> OrderByDesc(Expression<Func<T, object>> property) {
            this.QueryBase.OrderByDesc(property);

            return this;
        }

        /// <summary>
        /// Tells the query planner how to select the best index:
        /// by filtering or by sorting. The defult is the filtering
        /// priority, which should be used when a minority of the
        /// records are supposed to be returned. Use sorting priority
        /// when you expect to query back most of the records in
        /// a specific order.
        /// This setting is ignored if you hint a specific index
        /// using 'HintIndex'.
        /// </summary>
        public DeleteQuery<T> HintIndexPriority(IndexPriority priority) {
            this.QueryBase.HintIndexPriority(priority);

            return this;
        }

        /// <summary>
        /// Tells the query planner which index to use. Use the
        /// same name as in the annotation of the record class.
        /// If this option is set, then the 'HintIndexPriority'
        /// setting is ignored.
        /// </summary>
        public DeleteQuery<T> HintIndex(string indexName) {
            this.QueryBase.HintIndex(indexName);

            return this;
        }

        /// <summary>
        /// Returns a user-friendly text that describes the query plan.
        /// </summary>
        public string GetQueryPlan() {
            var plan = new QueryPlan<T>(this.QueryBase);

            return plan.ToString();
        }

        /// <summary>
        /// Save all changes onto the underlying device.
        /// </summary>
        /// <param name="garbageCollection">True = force garbage collection after the commit.</param>
        public void Commit(bool garbageCollection = false) {
            this.transaction.Commit(garbageCollection);
        }
    }
}
