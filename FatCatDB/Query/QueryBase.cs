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
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace FatCatDB {
    /// <summary>
    /// A query for fetching data from the database
    /// </summary>
    /// <typeparam name="T">An annotated class of a database table record</typeparam>
    internal class QueryBase<T> where T : class, new() {
        internal Table<T> Table { get; }
        internal Dictionary<int, string> IndexFilters { get; } = new Dictionary<int, string>();
        internal List<Func<T, bool>> FlexFilters { get; } = new List<Func<T, bool>>();
        private Bookmark bookmark = null;
        internal Bookmark Bookmark { get { return bookmark; } }
        private Int64 queryLimit = 0;
        internal Int64 QueryLimit { get { return queryLimit; } }
        internal List<Tuple<int, SortingDirection>> Sorting { get; } = new List<Tuple<int, SortingDirection>>();
        internal IndexPriority? IndexPriority = null;
        internal string HintedIndex = null;

        /// <summary>
        /// Constructor
        /// </summary>
        internal QueryBase(Table<T> table) {
            this.Table = table;
        }

        /// <summary>
        /// Fast filtering, using indexes. If you would like to filter using
        /// arbitrary expressions, then use the 'FlexFilter' method instead.
        /// The 'FlexFilter' is slower than the 'Where', because that's not
        /// using indexes.
        /// </summary>
        /// <param name="property">Filter by this column of the table</param>
        /// <param name="value">Exact match with this value</param>
        internal QueryBase<T> Where(Expression<Func<T, object>> property, object value) {
            IndexFilters[Table.GetPropertyIndex(Table.GetPropertyName(property))] = Table.ConvertValueToString(value);
            
            return this;
        }

        /// <summary>
        /// Generic filtering. In contrary to the 'Where' method, the 'FlexFilter' method
        /// doesn't use indexes, so its query time is linear, but it can handle
        /// arbitrary filter expressions. Please use it in combination with the
        /// 'Where' method for optimal performace.
        /// </summary>
        /// <param name="filterExpression">An arbitrary expression, involving the columns of the table.</param>
        internal QueryBase<T> FlexFilter(Func<T, bool> filterExpression) {
            this.FlexFilters.Add(filterExpression);

            return this;
        }

        /// <summary>
        /// Limit how many records to return for the
        /// query. Limit is disabled by default (limit = 0)
        /// Instead of an 'offset' FatCatDB uses bookmarks.
        /// See: Query.AfterBookmark(...)
        /// </summary>
        /// <param name="limit">How many items to return</param>
        internal QueryBase<T> Limit(Int64 limit) {
            this.queryLimit = limit;

            return this;
        }

        /// <summary>
        /// Continue a previous limited query after a given record.
        /// This functionality is similar to the "offset" of SQL.
        /// </summary>
        /// <param name="bookmark">A bookmark that was returned from a previous limited query.</param>
        internal QueryBase<T> AfterBookmark(string bookmark) {
            if (bookmark == null) {
                this.bookmark = null;
            } else {
                this.bookmark = Bookmark.FromString(bookmark);
            }
            
            return this;
        }

        /// <summary>
        /// Add a sorting directive for a specific field.
        /// Multiple field sorting is supported.
        /// </summary>
        internal QueryBase<T> OrderByAsc(Expression<Func<T, object>> property) {
            var pIndex = Table.GetPropertyIndex(Table.GetPropertyName(property));
            this.ValidateSortingProperty(pIndex);
            this.Sorting.Add(Tuple.Create(pIndex, SortingDirection.Ascending));

            return this;
        }

        /// <summary>
        /// Add a sorting directive for a specific field.
        /// Multiple field sorting is supported.
        /// </summary>
        internal QueryBase<T> OrderByDesc(Expression<Func<T, object>> property) {
            var pIndex = Table.GetPropertyIndex(Table.GetPropertyName(property));
            this.ValidateSortingProperty(pIndex);
            this.Sorting.Add(Tuple.Create(pIndex, SortingDirection.Descending));

            return this;
        }

        /// <summary>
        /// Checks whether the user tries to sort by a column of which
        /// type doesn't implement the IComparable interface.
        /// </summary>
        /// <param name="propertyIndex">The index of the property/column</param>
        private void ValidateSortingProperty(int propertyIndex) {
            if (!(typeof(IComparable).IsAssignableFrom(Table.GetRealPropertyType(propertyIndex)))) {
                var columnName = Table.ColumnNames[propertyIndex];
                throw new FatCatException($"Tried to order by column '{Table.Annotation.Name}.{columnName}'"
                    + " which has a type that doesn't implement the IComparable interface.");
            }
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
        internal QueryBase<T> HintIndexPriority(IndexPriority priority) {
            this.IndexPriority = priority;
            return this;
        }

        /// <summary>
        /// Tells the query planner which index to use. Use the
        /// same name as in the annotation of the record class.
        /// If this option is set, then the 'HintIndexPriority'
        /// setting is ignored.
        /// </summary>
        internal QueryBase<T> HintIndex(string indexName) {
            this.HintedIndex = indexName;
            return this;
        }

        /// <summary>
        /// Returns a user-friendly text that describes the query plan.
        /// </summary>
        internal string GetQueryPlan() {
            var plan = new QueryPlan<T>(this);

            return plan.ToString();
        }
    }
}
