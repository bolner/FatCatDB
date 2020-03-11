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
using System.Text.RegularExpressions;
using System.Collections.Generic;

namespace FatCatDB {
    internal class PathFilter<T> where T : class, new() {
        private Table<T> table;
        private int propertyIndex;
        private IComparable lower;
        private IComparable upper;
        private IComparable equals;
        private bool equalsNull = false;
        private List<Regex> regexes;

        /// <summary>
        /// Constructor
        /// </summary>
        internal PathFilter(Table<T> table, int propertyIndex) {
            this.table = table;
            this.propertyIndex = propertyIndex;
        }

        private IComparable ConvertIfRequired(IComparable value) {
            if (value is string) {
                return this.table.ConvertStringToValue(this.propertyIndex, (string)value);
            }

            return value;
        }

        /// <summary>
        /// Only this value satisfies the filter
        /// </summary>
        internal void EqualsValue(IComparable value) {
            if (value == null) {
                this.equalsNull = true;
                return;
            }

            this.equals = ConvertIfRequired(value);
        }

        /// <summary>
        /// Less than or equals
        /// </summary>
        internal void LessThanOrEquals(IComparable value) {
            if (value == null) {
                return;
            }

            if (this.upper == null || value.CompareTo(this.upper) < 0) {
                this.upper = ConvertIfRequired(value);
            }
        }

        /// <summary>
        /// Greater than or equals
        /// </summary>
        internal void GreaterThanOrEquals(IComparable value) {
            if (value == null) {
                return;
            }
            
            if (this.lower == null || value.CompareTo(this.lower) > 0) {
                this.lower = ConvertIfRequired(value);
            }
        }

        /// <summary>
        /// Inclusive interval
        /// </summary>
        internal void Between(IComparable lower, IComparable upper) {
            if (lower != null && upper != null && lower.CompareTo(upper) == 0) {
                this.EqualsValue(lower);
                return;
            }

            LessThanOrEquals(lower);
            GreaterThanOrEquals(upper);
        }

        /// <summary>
        /// Regex pattern matching
        /// </summary>
        internal void MatchRegEx(string pattern) {
            try {
                var regEx = new Regex(pattern);
                this.regexes.Add(regEx);
            }
            catch (Exception ex) {
                throw new ApplicationException($"Invalid regex pattern given for a filtering. Pattern: '{pattern}', "
                    + $"table: '{this.table.Annotation.Name}', column: '{this.table.ColumnNames[this.propertyIndex]}'. "
                    + $"Received error: '{ex.Message}'", ex);
            }
        }

        /// <summary>
        /// Test if a value passes the PathFilter or not.
        /// The value must be in the original column type,
        /// and not in string representation.
        /// </summary>
        internal bool Evaluate(IComparable value) {
            if (this.equalsNull) {
                return value == null;
            }

            if (this.equals != null) {
                return value.CompareTo(this.equals) == 0;
            }

            if (this.lower != null) {
                if (this.lower.CompareTo(value) > 0) {
                    return false;
                }
            }

            if (this.upper != null) {
                if (this.upper.CompareTo(value) < 0) {
                    return false;
                }
            }

            if (this.regexes.Count > 0) {
                string valueStr;

                if (value is string) {
                    valueStr = (string)value;
                } else {
                    valueStr = this.table.ConvertValueToString(this.propertyIndex, value);
                }

                foreach(var regEx in this.regexes) {
                    if (!regEx.IsMatch(valueStr)) {
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Returns true if maximum one value can satisfy the filter,
        /// false otherwise.
        /// </summary>
        internal bool IsStrict() {
            if (this.equalsNull || this.equals != null) {
                return true;
            }

            return false;
        }

        /// <summary>
        /// If 'IsStrict' returned true, then you can use this method
        /// to get the string representation of the only value
        /// that satisfies the filter.
        /// </summary>
        internal string GetStrictValueAsString() {
            if (this.equalsNull) {
                return this.table.NullValue;
            }

            return this.table.ConvertValueToString(this.propertyIndex, this.equals);
        }
    }
}
