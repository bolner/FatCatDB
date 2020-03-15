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
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace FatCatDB {
    /// <summary>
    /// A query for deleting records
    /// </summary>
    /// <typeparam name="T">An annotated class of a database table record</typeparam>
    public class Query<T> where T : class, new() {
        internal QueryBase<T> QueryBase { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public Query(Table<T> table) {
            this.QueryBase = new QueryBase<T>(table);
        }

        /// <summary>
        /// Fast filtering, using indices. If you would like to filter using
        /// arbitrary expressions, then use the 'FlexFilter' method instead.
        /// The 'FlexFilter' is slower than the 'Where', because that's not
        /// using indices.
        /// </summary>
        /// <param name="property">Filter by this column of the table</param>
        /// <param name="value">Exact match with this value</param>
        public Query<T> Where(Expression<Func<T, object>> property, IComparable value) {
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
        internal Query<T> WhereMin(Expression<Func<T, object>> property, IComparable value) {
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
        internal Query<T> WhereMax(Expression<Func<T, object>> property, IComparable value) {
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
        internal Query<T> WhereBetween(Expression<Func<T, object>> property, IComparable lower, IComparable upper) {
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
        internal Query<T> WhereRegEx(Expression<Func<T, object>> property, string pattern) {
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
        public Query<T> FlexFilter(Func<T, bool> filterExpression) {
            this.QueryBase.FlexFilter(filterExpression);

            return this;
        }

        /// <summary>
        /// Add a sorting directive for a specific field.
        /// Multiple field sorting is supported.
        /// </summary>
        public Query<T> OrderByAsc(Expression<Func<T, object>> property) {
            this.QueryBase.OrderByAsc(property);

            return this;
        }

        /// <summary>
        /// Add a sorting directive for a specific field.
        /// Multiple field sorting is supported.
        /// </summary>
        public Query<T> OrderByDesc(Expression<Func<T, object>> property) {
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
        public Query<T> HintIndexPriority(IndexPriority priority) {
            this.QueryBase.HintIndexPriority(priority);

            return this;
        }

        /// <summary>
        /// Tells the query planner which index to use. Use the
        /// same name as in the annotation of the record class.
        /// If this option is set, then the 'HintIndexPriority'
        /// setting is ignored.
        /// </summary>
        public Query<T> HintIndex(string indexName) {
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
        /// Returns a cursor which can iterate through
        /// the queried items.
        /// </summary>
        public Cursor<T> GetCursor() {
            return new Cursor<T>(this);
        }

        /// <summary>
        /// Returns the first record or null if none found.
        /// </summary>
        public T FindOne() {
            return this.GetCursor().FirstOrDefault();
        }

        /// <summary>
        /// Returns the first record or null if none found.
        /// </summary>
        public async Task<T> FindOneAsync() {
            return await this.GetCursor().FetchNextAsync();
        }

        /// <summary>
        /// Creates an Exporter instance
        /// </summary>
        public Exporter<T> GetExporter() {
            return new Exporter<T>(this.QueryBase.Table, this.GetCursor());
        }

        /// <summary>
        /// Prints out the query result to the standard output.
        /// </summary>
        public void Print() {
            this.GetExporter().Print();
        }

        /// <summary>
        /// Prints out the query result to the standard output.
        /// </summary>
        public async Task PrintAsync() {
            await this.GetExporter().PrintAsync();
        }

        /// <summary>
        /// Limit how many records to return for the
        /// query. Limit is disabled by default (limit = 0)
        /// Instead of an 'offset' FatCatDB uses bookmarks.
        /// See: Query.AfterBookmark(...)
        /// </summary>
        /// <param name="limit">How many items to return</param>
        public Query<T> Limit(Int64 limit) {
            this.QueryBase.Limit(limit);

            return this;
        }

        /// <summary>
        /// Continue a previous limited query after a given record.
        /// This functionality is similar to the "offset" of SQL.
        /// </summary>
        /// <param name="bookmark">A bookmark that was returned from a previous limited query.</param>
        public Query<T> AfterBookmark(string bookmark) {
            this.QueryBase.AfterBookmark(bookmark);
            
            return this;
        }
    }
}
