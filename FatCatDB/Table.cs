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
using System.Reflection;
using System.Linq.Expressions;
using System.Collections.Generic;

namespace FatCatDB {
    /// <summary>
    /// Database table
    /// </summary>
    /// <typeparam name="T">The type of the records in the table. Annotated schema definition.</typeparam>
    public class Table<T> : TableBase where T : class, new() {
        private bool initialized = false;

        /// <summary>
        /// For reflection on the record class
        /// </summary>
        private Type recordType;

        private DbContextBase dbContext;
        /// <summary>
        /// The Database Context, created by the user
        /// </summary>
        public DbContextBase DbContext { get { return dbContext; } }

        private Annotation.Table annotation;
        internal Annotation.Table Annotation { get { return annotation; } }

        private PropertyInfo[] properties;
        /// <summary>
        /// The properties from the annotated class of the database table record
        /// </summary>
        internal PropertyInfo[] Properties { get { return properties; }}

        private List<int> uniquePropertyIndices = new List<int>();

        private List<TableIndex<T>> indices = new List<TableIndex<T>>();
        private Dictionary<string, int> propertyNameToIndex = new Dictionary<string, int>();
        private Dictionary<string, int> columnNameToIndex = new Dictionary<string, int>();

        private Func<IComparable, IComparable>[] conversionTable_ToStr;
        private Func<IComparable, IComparable>[] conversionTable_FromStr;

        /// <summary>
        /// The names of the columns (which are written in the annotations)
        /// </summary>
        private string[] columnNames;
        internal string[] ColumnNames { get { return columnNames; }}

        /// <summary>
        /// Null values are stored in the DB as this text.
        /// The default can be changed by a Table(NullValue="") annotation on the table.
        /// </summary>
        internal string NullValue { get { return nullValue; }}
        private string nullValue = "";
        
        /// <summary>
        /// This is called by the DbContextBase after it registered
        /// all its tables.
        /// </summary>
        internal override void Initialize(DbContextBase dbContext) {
            if (initialized) {
                return;
            }

            this.initialized = true;
            this.dbContext = dbContext;
            this.recordType = typeof(T);
            var columnLookup = new Dictionary<string, PropertyInfo>();
            int indexCount = 0;
            var propTmp = new List<PropertyInfo>();
            var cNamesTmp = new List<string>();

            foreach(var prop in recordType.GetProperties()) {
                foreach(var att in prop.GetCustomAttributes(true)) {
                    if (att is Annotation.Column) {
                        Annotation.Column cAtt = (Annotation.Column)att;
                        if (cAtt.Name == null) {
                            throw new FatCatException($"In class '{recordType.Name}' the 'Name' field is missing for property '{prop.Name}'.");
                        }
                        string columnName = cAtt.Name.Trim();
                        if (columnName == "") {
                            throw new FatCatException($"In class '{recordType.Name}' the 'Name' field is empty for property '{prop.Name}'.");
                        }

                        if (columnLookup.ContainsKey(columnName)) {
                            throw new FatCatException($"In class '{recordType.Name}' more than one column has the name '{cAtt.Name}'.");
                        }

                        cNamesTmp.Add(columnName);
                        propTmp.Add(prop);
                        columnLookup[columnName] = prop;
                        columnNameToIndex[columnName] = indexCount;
                        propertyNameToIndex[prop.Name] = indexCount;
                        indexCount++;
                    }
                }
            }

            this.columnNames = cNamesTmp.ToArray();
            this.properties = propTmp.ToArray();

            var attributes = recordType.GetCustomAttributes(true);
            indexCount = 0;

            foreach(var att in attributes) {
                if (att is Annotation.TableIndex) {
                    Annotation.TableIndex indexAtt = (Annotation.TableIndex)att;
                    var props = new List<int>();

                    foreach(var name in indexAtt.Columns.Split(',').Select(x => x.Trim()).ToList()) {
                        if (name == "") {
                            throw new FatCatException($"In class '{recordType.Name}' the column list for the index '{indexAtt.Name}' has invalid syntax. Please use comma separated columns names.");
                        }

                        if (!columnLookup.ContainsKey(name)) {
                            throw new FatCatException($"In class '{recordType.Name}' the index '{indexAtt.Name}' refers to column '{name}' which doesn't exist.");
                        }

                        props.Add(columnNameToIndex[name]);
                    }

                    indices.Add(
                        new TableIndex<T>(this, recordType.Name, indexAtt.Name, props, indexCount)
                    );
                    indexCount++;
                }
                else if (att is Annotation.Table) {
                    Annotation.Table tableAtt = (Annotation.Table)att;
                    
                    foreach(var name in tableAtt.Unique.Split(',').Select(x => x.Trim()).ToList()) {
                        if (!columnLookup.ContainsKey(name)) {
                            throw new FatCatException($"In class '{recordType.Name}' the unique field of the table annotation refers to column '{name}' which doesn't exist.");
                        }

                        uniquePropertyIndices.Add(columnNameToIndex[name]);
                    }

                    nullValue = tableAtt.NullValue;
                    annotation = tableAtt;

                    if (nullValue == null) {
                        nullValue = "";
                    }
                }
            }

            foreach(var prop in Properties) {
                if (prop.GetType().IsValueType && Nullable.GetUnderlyingType(prop.GetType()) == null) {
                    throw new Exception($"In class '{recordType.Name}' the property '{prop.Name}' is not nullable. All properties"
                        + $" have to be nullables. For example you can use 'Nullable<int>' for an 'int' type.");
                }

                var ptype = Nullable.GetUnderlyingType(prop.PropertyType);
                if (ptype == null) {
                    ptype = prop.PropertyType;
                }

                if (!(typeof(IComparable).IsAssignableFrom(ptype))) {
                    throw new FatCatException($"In class '{recordType.Name}' the property '{prop.Name}' "
                        + $"has a type '{prop.PropertyType.Name}' that doesn't implement the IComparable interface. "
                        + "All annotated columns must be sortable.");
                }
            }

            if (indices.Count < 1) {
                throw new Exception($"Class '{recordType.Name}' has no TableIndex annotations. Please add at least one index.");
            }

            /*
                Generate the conversion tables
            */
            conversionTable_ToStr = new Func<IComparable, IComparable>[Properties.Length];
            conversionTable_FromStr = new Func<IComparable, IComparable>[Properties.Length];
            int index = 0;
            var converter = this.DbContext.TypeConverter;

            foreach(var prop in Properties) {
                var pType = Nullable.GetUnderlyingType(prop.PropertyType);
                if (pType == null) {
                    pType = prop.PropertyType;
                }

                if (pType.Name == typeof(string).Name) {
                    // Already string => no conversion
                    conversionTable_ToStr[index] = null;
                    conversionTable_FromStr[index] = null;
                } else {
                    conversionTable_ToStr[index] = converter.GetConverter(pType.Name, typeof(string).Name);
                    conversionTable_FromStr[index] = converter.GetConverter(typeof(string).Name, pType.Name);
                }

                index++;
            }
        }

        /// <summary>
        /// For nullable types it returns the underlying type, for normal
        /// types it just returns the type of the property.
        /// </summary>
        /// <param name="propertyIndex">Numeric index that identifies the column/property.</param>
        /// <returns>A C# type</returns>
        public Type GetRealPropertyType(int propertyIndex) {
            var pType1 = Properties[propertyIndex].PropertyType;
            var pType2 = Nullable.GetUnderlyingType(pType1);
            if (pType2 != null) {
                return pType2;
            }

            return pType1;
        }

        /// <summary>
        /// Creates a new query
        /// </summary>
        public Query<T> Query() {
            return new Query<T>(this);
        }

        internal List<TableIndex<T>> GetIndices() {
            return indices;
        }

        /// <summary>
        /// Creates a new transaction
        /// </summary>
        public Transaction<T> NewTransaction() {
            return new Transaction<T>(this);
        }

        /// <summary>
        /// Populates the output array with the values in the columns
        /// of the record. Converts all values to strings in a reliable way.
        /// </summary>
        /// <param name="record">The source of the data</param>
        /// <param name="output">The result is written to here</param>
        internal void GetStringValues(object record, string[] output) {
            int length = Math.Min(Properties.Length, output.Length);

            for (int i = 0; i < length; i++) {
                output[i] = ConvertValueToString(i, (IComparable)Properties[i].GetValue(record));
            }
        }

        internal string ConvertValueToString(int propertyIndex, IComparable value) {
            if (value == null) {
                return nullValue;
            }

            if (conversionTable_ToStr[propertyIndex] == null) {
                // Already string => no conversion, only casting
                return (string)value;
            }

            try {
                return (string)conversionTable_ToStr[propertyIndex](value);
            } catch (Exception ex) {
                throw new FatCatException($"Failed to convert value for column '{this.ColumnNames[propertyIndex]}' to "
                    + $"string. The following error was thrown: '{ex.Message}'. Please double check the type "
                    + "conversion functions for the type of that specific column.", ex);
            }
        }

        /// <summary>
        /// This is a slow function that whould not be used in the
        /// serialization of records.
        /// </summary>
        internal IComparable ConvertStringToValue(int propertyIndex, string value) {
            if (value == nullValue) {
                return null;
            }

            if (conversionTable_FromStr[propertyIndex] == null) {
                // Same type (both string)
                return value;
            }

            try {
                return conversionTable_FromStr[propertyIndex](value);
            } catch (Exception ex) {
                throw new FatCatException($"Failed to convert value '{value}' for column '{this.ColumnNames[propertyIndex]}' to "
                    + $"its original type. The following error was thrown: '{ex.Message}'. Please double check the type "
                    + "conversion functions for the type of that specific column.", ex);
            }
        }

        internal int GetPropertyIndex(Expression<Func<T, object>> property) {
            string name = null;
            object x = property.Body;

            if (x is UnaryExpression) {
                x = ((UnaryExpression)x).Operand;
            }

            if (x is MemberExpression) {
                name = ((MemberExpression)x).Member.Name;
            }

            if (name == null) {
                throw new FatCatException("Expected a 'Property' in a lambda expression, but received something else.");
            }

            int propertyIndex = -1;
            propertyNameToIndex.TryGetValue(name, out propertyIndex);

            if (propertyIndex == -1) {
                throw new FatCatException($"Unknown property '{name}' passed in a lambda expression for table '{this.Annotation.Name}'.");
            }

            return propertyIndex;
        }

        /// <summary>
        /// Loads a line of TSV data into this object.
        /// Ignores fields which have no mappings currently.
        /// Length match between header and line has to be guaranteed, because
        /// this method doesn't know enough for a nice error message.
        /// </summary>
        /// <param name="TsvMapping"></param>
        /// <param name="record">Target object</param>
        /// <param name="line">Data from a TSV line per field</param>
        internal void LoadFromTSVLine(TsvMapping<T> TsvMapping, object record, List<string> line) {
            for (int propertyIndex = 0; propertyIndex < Properties.Length; propertyIndex++) {
                Nullable<int> lineIndex = TsvMapping.FromRecordToTsv[propertyIndex];

                if (lineIndex == null) {
                    Properties[propertyIndex].SetValue(record, null);
                } else {
                    Properties[propertyIndex].SetValue(record, ConvertStringToValue(propertyIndex, line[(int)lineIndex]));
                }
            }            
        }

        /// <summary>
        /// Returns the values of the primary key fields concatenated
        /// as a single string value.
        /// </summary>
        internal string GetUnique(object record) {
            var values = new List<string>();

            foreach(int propertyIndex in uniquePropertyIndices) {
                values.Add(
                    ConvertValueToString(propertyIndex, (IComparable)Properties[propertyIndex].GetValue(record))
                );
            }

            return String.Join('\0', values);
        }

        /// <summary>
        /// Returns the values of the index fields.
        /// </summary>
        internal List<string> GetIndexPath(TableIndex<T> index, object record) {
            var path = new List<string>();

            foreach(int propertyIndex in index.PropertyIndices) {
                path.Add(
                    ConvertValueToString(propertyIndex, (IComparable)Properties[propertyIndex].GetValue(record))
                );
            }

            return path;
        }

        /// <summary>
        /// Returns the values of the index fields.
        /// </summary>
        internal Dictionary<string, string> GetIndexPathAssoc(TableIndex<T> index, object record) {
            var path = new Dictionary<string, string>();

            foreach(int propertyIndex in index.PropertyIndices) {
                path[ColumnNames[propertyIndex]] = ConvertValueToString(
                    propertyIndex,
                    (IComparable)Properties[propertyIndex].GetValue(record)
                );
            }

            return path;
        }

        /// <summary>
        /// Returns key/value pairs that fully identify a record in respect to the
        /// selected index and its unique key.
        /// </summary>
        internal Dictionary<string, string> GetFullRecordPath(TableIndex<T> index, object record) {
            var path = this.GetIndexPathAssoc(index, record);

            foreach(int propertyIndex in uniquePropertyIndices) {
                path[ColumnNames[propertyIndex]] = ConvertValueToString(
                    propertyIndex,
                    (IComparable)Properties[propertyIndex].GetValue(record)
                );
            }

            return path;
        }

        /// <summary>
        /// Returns the corresponding property index for a column name if found.
        /// Returns -1 otherwise.
        /// </summary>
        internal int ColumnNameToPropertyIndex(string columnName) {
            int value = -1;
            this.columnNameToIndex.TryGetValue(columnName, out value);

            return value;
        }
    }
}
