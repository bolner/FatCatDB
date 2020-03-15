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
    public class UpdateQuery<T> where T : class, new() {
        internal QueryBase<T> QueryBase { get; }
        private Transaction<T> transaction;

        /// <summary>
        /// Constructor
        /// </summary>
        public UpdateQuery(Table<T> table, Transaction<T> transaction) {
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
        public UpdateQuery<T> Where(Expression<Func<T, object>> property, IComparable value) {
            this.QueryBase.Where(property, value);
            
            return this;
        }

        /// <summary>
        /// Greater than or equals.
        /// Fast filtering, using indices. If you would like to filter using
        /// arbitrary expressions, then use the 'FlexFilter' method instead.
        /// The 'FlexFilter' is slower than the 'Where' methods, because
        /// that's not using indices.
        /// </summary>
        /// <param name="property">Filter by this column of the table</param>
        /// <param name="value">Campare to this value</param>
        internal UpdateQuery<T> WhereMin(Expression<Func<T, object>> property, IComparable value) {
            this.WhereMin(property, value);
            
            return this;
        }

        /// <summary>
        /// Less than or equals.
        /// Fast filtering, using indices. If you would like to filter using
        /// arbitrary expressions, then use the 'FlexFilter' method instead.
        /// The 'FlexFilter' is slower than the 'Where' methods, because
        /// that's not using indices.
        /// </summary>
        /// <param name="property">Filter by this column of the table</param>
        /// <param name="value">Campare to this value</param>
        internal UpdateQuery<T> WhereMax(Expression<Func<T, object>> property, IComparable value) {
            this.WhereMax(property, value);
            
            return this;
        }

        /// <summary>
        /// Inclusive interval filtering.
        /// Fast filtering, using indices. If you would like to filter using
        /// arbitrary expressions, then use the 'FlexFilter' method instead.
        /// The 'FlexFilter' is slower than the 'Where' methods, because
        /// that's not using indices.
        /// </summary>
        /// <param name="property">Filter by this column of the table</param>
        /// <param name="lower">Lowest value of the interval</param>
        /// <param name="upper">Highest value of the interval</param>
        internal UpdateQuery<T> WhereBetween(Expression<Func<T, object>> property, IComparable lower, IComparable upper) {
            this.WhereBetween(property, lower, upper);
            
            return this;
        }

        /// <summary>
        /// Regular expression-based pattern filtering on the string
        /// representation of the values.
        /// Fast filtering, using indices. If you would like to filter using
        /// arbitrary expressions, then use the 'FlexFilter' method instead.
        /// The 'FlexFilter' is slower than the 'Where' methods, because
        /// that's not using indices.
        /// </summary>
        /// <param name="property">Filter by this column of the table</param>
        /// <param name="pattern">Regular expression pattern</param>
        internal UpdateQuery<T> WhereRegEx(Expression<Func<T, object>> property, string pattern) {
            this.WhereRegEx(property, pattern);
            
            return this;
        }

        /// <summary>
        /// Generic filtering. In contrary to the 'Where' method, the 'FlexFilter' method
        /// doesn't use indices, so its query time is linear, but it can handle
        /// arbitrary filter expressions. Please use it in combination with the
        /// 'Where' method for optimal performace.
        /// </summary>
        /// <param name="filterExpression">An arbitrary expression, involving the columns of the table.</param>
        public UpdateQuery<T> FlexFilter(Func<T, bool> filterExpression) {
            this.QueryBase.FlexFilter(filterExpression);

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
