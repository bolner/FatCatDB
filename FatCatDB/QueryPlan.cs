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
using System.Text;
using System.Linq;
using System.Collections.Generic;

namespace FatCatDB {
    internal class QueryPlan<T> where T : class, new() {
        internal Table<T> Table;
        internal QueryBase<T> Query { get; }
        internal Dictionary<int, PathFilter<T>> FreeIndexFilters { get; } = new Dictionary<int, PathFilter<T>>();
        internal Dictionary<int, SortingDirection> SortingAssoc { get; } = new Dictionary<int, SortingDirection>();
        internal TableIndex<T> BestIndex { get; }
        internal HashSet<int> IndexFields = new HashSet<int>();
        internal Dictionary<int, SortingDirection> BoundSorting { get; } = new Dictionary<int, SortingDirection>();
        internal List<Tuple<int, SortingDirection>> FreeSorting { get; } = new List<Tuple<int, SortingDirection>>();

        internal QueryPlan(QueryBase<T> query) {
            this.Table = query.Table;
            this.Query = query;
            
            if (query.HintedIndex != null) {
                this.BestIndex = Table.GetIndices().Where(x => x.Name == query.HintedIndex).First();
                if (this.BestIndex == null) {
                    throw new FatCatException($"An index with name '{query.HintedIndex}' has been hinted for the query planner, "
                        + $"but there is no index by that name in the annotations of table '{Table.Annotation.Name}'.");
                }
            }
            else if (query.IndexPriority == null) {
                this.BestIndex = FindBestIndex(IndexPriority.Filtering);
            }
            else {
                this.BestIndex = FindBestIndex((IndexPriority)query.IndexPriority);
            }
            
            foreach(int propIndex in BestIndex.PropertyIndices) {
                this.IndexFields.Add(propIndex);
            }
            
            /*
                Partition the filters
            */
            foreach(var item in query.IndexFilters) {
                if (!IndexFields.Contains(item.Key)) {
                    this.FreeIndexFilters[item.Key] = item.Value;
                }
            }

            /*
                Partition the sorting directives
            */
            foreach(var sortingDirective in query.Sorting) {
                this.SortingAssoc[sortingDirective.Item1] = sortingDirective.Item2;

                if (!IndexFields.Contains(sortingDirective.Item1)) {
                    this.FreeSorting.Add(sortingDirective);
                }
            }

            /*
                Validate if sorting is possible
            */
            int sortingLevel = 0;

            foreach(int propIndex in BestIndex.PropertyIndices) {
                if (sortingLevel >= query.Sorting.Count) {
                    break;
                }

                if (query.IndexFilters.ContainsKey(propIndex)) {
                    // Index level is fixed by a filter
                    continue;
                }
                else if (propIndex == query.Sorting[sortingLevel].Item1) {
                    // Index level is ordered
                    BoundSorting[propIndex] = query.Sorting[sortingLevel].Item2;
                    sortingLevel++;
                    continue;
                }
                else if (SortingAssoc.ContainsKey(propIndex)) {
                    var currentSorting = new List<string>();
                    var recommend = new List<string>();

                    foreach(var directive in Query.Sorting) {
                        currentSorting.Add(Table.ColumnNames[directive.Item1]);
                    }

                    foreach(var index in FreeIndexFilters.Keys) {
                        recommend.Add(Table.ColumnNames[index]);
                    }

                    foreach(var item in FreeSorting) {
                        recommend.Add(Table.ColumnNames[item.Item1]);
                    }

                    throw new FatCatException($"Unable to apply sorting by the tuple <{String.Join(", ", currentSorting)}>."
                        + $" Either change sorting to a subset of <{String.Join(", ", recommend)}> (the order is important!),"
                        + $" or force another index though query hinting. (The currently selected index is '{BestIndex.Name}'.)");
                }
            }
        }

        /// <summary>
        /// Finds the best index by giving priority to "index filtering" over sorting.
        /// </summary>
        /// <returns></returns>
        private TableIndex<T> FindBestIndex(IndexPriority prio) {
            var indices = Table.GetIndices();
            var orderBy = Query.Sorting;
            var indexFilters = Query.IndexFilters;

            indices.Sort((i2, i1) => {
                int indexLevel = 0;
                Nullable<int> indexProperty1, indexProperty2;
                bool canBeFiltered1, canBeFiltered2;
                int sortingLevel1 = 0, sortingLevel2 = 0;
                bool sortingCanApply1, sortingCanApply2;
                
                /*
                    Search for the first decisive level
                */
                do {
                    sortingCanApply1 = false;
                    sortingCanApply2 = false;

                    if (i1.PropertyIndices.Count > indexLevel) {
                        // The level is still valid for index 1
                        indexProperty1 = i1.PropertyIndices[indexLevel];
                        canBeFiltered1 = indexFilters.ContainsKey((int)indexProperty1);

                        // Independently check if sorting is possible
                        if (orderBy.Count > sortingLevel1) {
                            if (orderBy[sortingLevel1].Item1 == indexProperty1) {
                                sortingCanApply1 = true;
                                // A sorting directive can only match if the previous
                                //  ones were matching too.
                                sortingLevel1++;
                            }
                        }
                    } else {
                        // Index 1 has run out
                        canBeFiltered1 = false;
                        indexProperty1 = null;
                    }
                    
                    if (i2.PropertyIndices.Count > indexLevel) {
                        // The level is still valid for index 2
                        indexProperty2 = i2.PropertyIndices[indexLevel];
                        canBeFiltered2 = indexFilters.ContainsKey((int)indexProperty2);

                        // Independently check if sorting is possible
                        if (orderBy.Count > sortingLevel2) {
                            if (orderBy[sortingLevel2].Item1 == indexProperty2) {
                                sortingCanApply2 = true;
                                // A sorting directive can only match if the previous
                                //  ones were matching too.
                                sortingLevel2++;
                            }
                        }
                    } else {
                        // Index 2 has run out
                        canBeFiltered2 = false;
                        indexProperty2 = null;
                    }

                    // Both indexes ran out
                    if (indexProperty1 == null && indexProperty2 == null) {
                        break;
                    }

                    // They are the same => no winner on this level
                    if (indexProperty1 == indexProperty2) {
                        indexLevel++;
                        continue;
                    }

                    if (prio == IndexPriority.Filtering) {
                        /*
                            If only one of the properties is used in an
                            "index filtering" (or "Where") expression,
                            then its index wins.
                        */
                        if (canBeFiltered1 && !canBeFiltered2) {
                            return 1;
                        }
                        if (canBeFiltered2 && !canBeFiltered1) {
                            return -1;
                        }

                        /*
                            If none or both of the fields in the current index level
                            are filtered by a "Where" statement, then check if
                            any of the sorting directives apply.
                        */
                        if (sortingCanApply1 && !sortingCanApply2) {
                            return 1;
                        }
                        if (sortingCanApply2 && !sortingCanApply1) {
                            return -1;
                        }
                    } else {
                        /*
                            If only one of them is used on the current
                            sorting level, then it wins.
                        */
                        if (sortingCanApply1 && !sortingCanApply2) {
                            return 1;
                        }
                        if (sortingCanApply2 && !sortingCanApply1) {
                            return -1;
                        }

                        /*
                            If none or both fields can be filtered,
                            then check is "index filtering" can
                            be applied.
                        */
                        if (canBeFiltered1 && !canBeFiltered2) {
                            return 1;
                        }
                        if (canBeFiltered2 && !canBeFiltered1) {
                            return -1;
                        }
                    }

                    indexLevel++;
                } while(true);

                /*
                    If it's not possible to choose between the two,
                    then choose the one which precedes the other
                    in the list of index annotations at the
                    table definition. As it is expected that the
                    user ordered the indexes in a smart way.
                    (Rank 0 is the first)
                */
                return i2.Rank - i1.Rank;
            });

            return indices.First();
        }

        /// <summary>
        /// Generates a user-friendly text of the query plan
        /// </summary>
        public override string ToString() {
            var sb = new StringBuilder();

            if (Query.HintedIndex != null) {
                sb.AppendLine($"- A specific index was hinted in the query.");
            }
            else if (Query.IndexPriority == null) {
                sb.AppendLine($"- The default index selection mode was selected which gives priority to filtering over sorting.");
            }
            else if (Query.IndexPriority == IndexPriority.Filtering) {
                sb.AppendLine($"- An index selection mode was hited which gives priority to filtering over sorting.");
            }
            else {
                sb.AppendLine($"- An index selection mode was hited which gives priority to sorting over filtering.");
            }

            sb.AppendLine($"- The selected index is '{this.BestIndex.Name}'. The steps of the query are:");
            sb.AppendLine($"    - Index levels:");
            int level = 1;

            foreach(var propIndex in this.BestIndex.PropertyIndices) {
                string column = Table.ColumnNames[propIndex];
                string operation = "Full scan (unsorted)";

                if (this.Query.IndexFilters.ContainsKey(propIndex)) {
                    operation = "Select one (exact match)";
                }
                else if (this.SortingAssoc.ContainsKey(propIndex)) {
                    operation = "Sort by (full scan)";
                }

                sb.AppendLine($"        - {level}. {column}: {operation}");
                level++;
            }

            if (this.Query.FlexFilters.Count > 0) {
                sb.AppendLine($"    - Apply flex filtering.");
            }

            if (this.FreeIndexFilters.Count > 0) {
                sb.AppendLine($"    - Apply the 'Where' filters, which weren't used for an index level.");
            }

            if (FreeSorting.Count > 0) {
                var sortingColumns = FreeSorting.Select(
                    x => Table.ColumnNames[x.Item1]
                ).ToArray();
                
                sb.AppendLine($"    - Apply the sorting directives inside the packets, which weren't used for an index level:");

                foreach(var col in sortingColumns) {
                    sb.AppendLine($"        - {col}");
                }
            }

            if (Query.Bookmark != null) {
                sb.AppendLine($"    - Bookmark: Start after a specific record. (Paging)");
            }

            if (Query.QueryLimit > 0) {
                sb.AppendLine($"    - Limit: The maximal number of records to return is {Query.QueryLimit}");
            }

            if (Query.Bookmark == null && Query.QueryLimit == 0) {
                sb.AppendLine($"    - Return the complete result.");
            }

            return sb.ToString();
        }
    }
}
