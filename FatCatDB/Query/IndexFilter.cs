using System;

namespace FatCatDB {
    internal class IndexFilter {
        internal IndexFilterOperator Operator { get; }
        internal IComparable Value1 { get; }
        internal IComparable Value2 { get; }

        /// <summary>
        /// Constructor for "Where"
        /// </summary>
        /// <param name="value">Equals with value. It can be NULL.</param>
        internal IndexFilter(IComparable value) {
            this.Operator = IndexFilterOperator.Equals_Value1;
            this.Value1 = value;
        }

        /// <summary>
        /// Constructor for "Between"
        /// </summary>
        /// <param name="start">Start interval. If null then open at the bottom. (less than)</param>
        /// <param name="end">End interval. If null then open at the top. (greater than)</param>
        internal IndexFilter(IComparable start, IComparable end) {
            if (start == null && end == null) {
                throw new FatCatException("Invalid 'Between' directive. Both operands are null.");
            }
            else if (start == null) {
                this.Operator = IndexFilterOperator.Before_Value2;
                this.Value2 = end;
            }
            else if (end == null) {
                this.Operator = IndexFilterOperator.After_Value1;
                this.Value1 = start;
            }
            else {
                this.Operator = IndexFilterOperator.Between_Value1_Value2;
                this.Value1 = start;
                this.Value2 = end;
            }
        }

        internal bool IsIntersectedBy(IComparable value, bool invertOrder = false) {
            if (this.Operator == IndexFilterOperator.Equals_Value1) {
                if (this.Value1 == null) {
                    return value == null;
                }
                else if (value == null) {
                    return false;
                }
                else {
                    return this.Value1.CompareTo(value) == 0;
                }
            }

            if (value == null) {
                return false;
            }

            if (invertOrder) {
                if (this.Operator == IndexFilterOperator.After_Value1) {
                    return this.Value1.CompareTo(value) >= 0;
                }

                if (this.Operator == IndexFilterOperator.Before_Value2) {
                    return this.Value2.CompareTo(value) <= 0;
                }

                return this.Value1.CompareTo(value) >= 0
                        && this.Value2.CompareTo(value) <= 0;
            }

            if (this.Operator == IndexFilterOperator.After_Value1) {
                return this.Value1.CompareTo(value) <= 0;
            }

            if (this.Operator == IndexFilterOperator.Before_Value2) {
                return this.Value2.CompareTo(value) >= 0;
            }

            return this.Value1.CompareTo(value) <= 0
                    && this.Value2.CompareTo(value) >= 0;
        }
    }
}
