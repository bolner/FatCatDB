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

namespace FatCatDB {
    /// <summary>
    /// The direction of a sorting directive
    /// </summary>
    internal enum SortingDirection {
        /// <summary>
        /// Sort by ascending order
        /// </summary>
        Ascending,

        /// <summary>
        /// Sort by descending order
        /// </summary>
        Descending
    }

    /// <summary>
    /// Chooses the way how the best index is selected.
    /// Use "filtering priority" when a small portion of the
    /// full data set is expected to be returned by the query,
    /// and "sorting priority" when the majority of the data,
    /// and the query has sorting directives.
    /// </summary>
    public enum IndexPriority {
        /// <summary>
        /// Select an index that is the best for the 'Where'
        /// filtering expressions.
        /// </summary>
        Filtering,

        /// <summary>
        /// Select an index that is the best for the
        /// sorting directives.
        /// </summary>
        Sorting
    }

    internal enum IndexFilterOperator {
        Equals_Value1,
        After_Value1,
        Before_Value2,
        Between_Value1_Value2
    }
}
