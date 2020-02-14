FatCatDB
========

`- This project is still in its beta phase. If you want a well tested version, please come back in April. -`

FatCatDB is a `zero configuration` database library for `.NET Core`. Its main target segment is `ETL workflows` (e.g. time-series data), therefore it's optimized for high throughput. Supports class-based [schema definition](#creating-a-table-schema), multiple indices per table and fluid, object-oriented query expressions. One would use it for a smaller project to avoid managing a PostgreSQL or another full-fledged database system. With this library your project will already have data storage capability after just cloned from a GIT repo. You don't need to create and maintain Docker images for a database server.

# Example query

You can make fluid style queries using lambda expressions:

```csharp
var db = new DbContext();
var cursor = db.Metrics.Query()
    .Where(x => x.Date, "2020-01-02")
    .Where(x => x.AccountID, "a11")
    .FlexFilter(x => x.Revenue > x.Cost * 2.2 && x.Impressions > 10)
    .OrderByAsc(x => x.CampaignID)
    .OrderByDesc(x => x.Cost)
    .GetCursor();

foreach(var item in cursor) {
    ...
}
```

# Table of contents

- [FatCatDB](#fatcatdb)
- [Example query](#example-query)
- [Table of contents](#table-of-contents)
- [NuGet package](#nuget-package)
- [Creating a table schema](#creating-a-table-schema)
- [Creating a database context class](#creating-a-database-context-class)
- [Inserting and modifying data](#inserting-and-modifying-data)
- [Queries](#queries)
- [Atomic operations with the OnUpdate event](#atomic-operations-with-the-onupdate-event)
- [Making fields unchangeable using OnUpdate](#making-fields-unchangeable-using-onupdate)
- [Async support](#async-support)
- [Adding new types](#adding-new-types)
- [Hinting the query planner](#hinting-the-query-planner)
- [ACID and durability](#acid-and-durability)
- [Configurations](#configurations)
- [TODO](#todo)

# NuGet package

Available at: https://www.nuget.org/packages/FatCatDB

To include it in a `.NET Core` project, execute:

```bash
$ dotnet add package FatCatDB
```

# Creating a table schema

See the below example. You only have to add annotations to a class and some of its public properties. See an explanation below the example. As you see all annotated columns must have `Nullable` type, which you can either achieve by adding a question mark after non-nullable types, like `long?` or through `Nullable<long>`.

```csharp
using System;
using FatCatDB.Annotation;
using NodaTime;

namespace FatCatDB.Test {
    [Table(Name = "test_event", Unique = "campaign_id, ad_id", NullValue = "n.a.")]
    [TableIndex(Name = "account_date", Columns = "account_id, date")]
    [TableIndex(Name = "date_account", Columns = "date, account_id")]
    internal class MetricsRecord {
        [Column(Name = "date")]
        public LocalDate Date { get; set; }

        [Column(Name = "account_id")]
        public string AccountID { get; set; }

        [Column(Name = "campaign_id")]
        public string CampaignID { get; set; }

        [Column(Name = "ad_id")]
        public string AdID { get; set; }

        [Column(Name = "last_updated")]
        public LocalDateTime LastUpdated { get; set; }

        [Column(Name = "impressions")]
        public long? Impressions { get; set; }

        [Column(Name = "clicks")]
        public long? Clicks { get; set; }

        [Column(Name = "conversion")]
        public long? Conversions { get; set; }

        [Column(Name = "revenue")]
        public double? Revenue { get; set; }

        [Column(Name = "cost")]
        public double? Cost { get; set; }
    }
}
```

Annotation | Description
--- | ---
`Table.Name` | The name of the database table. Used in error messages and in the filesystem structure.
`Table.Unique` | Each `TableIndex` defines a way to partition the data into packets. This `Unique` property defines uniqueness inside a packet only. Do not use the same list of fields here as in any of the `TableIndex` annotations, because you would end up with packets containing a single record. Think of this as a continuation of the indices.
`Table.NullValue` | The string representation of how to store "unknown" or NULL values.
`TableIndex.Name` | The name of a database table index which speeds up queries. If you define 3 indices, then the data is stored 3 times on the disk, redundantly.
`TableIndex.Columns` | Comma-separated list of columns. This works the same way how you define composite indices in a relational database. In FatCatDB an index defines a multi-level directory structure on the disk, which contain `.tsv.gz` files, called `packets`. The column list tells how to partition the data into packets. The optimal size of a packet is around between 10 KB -> 1 MB. This database uses multi-level directory structures for quick queries.
`Column.Name` | If you want a property to be part of the database table, then add a `Column` annotation to it. The `Name` tells how the `Table.Unique` and the `TableIndex.Columns` fields refer to it. The data is also exported by default on this name.

All properties without annotation are just ignored by FatCatDB, and they won't cause any problem. Feel free to include arbitrary logic (methods, custom properties, private members, etc.) in your record classes.

# Creating a database context class

The design of FatCatDB follows [dependency injection](https://en.wikipedia.org/wiki/Dependency_injection) to make implementing unit tests possible. Therefore to use it, you have to instantiate a `datase context` class, which is derived from `DbContextBase`.

```csharp
internal class DbContext : DbContextBase {
    public Table<MetricsRecord> Metrics { get; } = new Table<MetricsRecord>();
    
    protected override void OnConfiguring (TypeConverterSetup typeConverterSetup, Configurator configurator) {
        configurator
            .SetTransactionParallelism(8)
            .SetQueryParallelism(8)
            .EnableDurability(false);
    }
}
```

Two imporant things to note:
- All tables must be defined inside the context class the above way. As a property with a `get` accessor, and also setting it to an instance with the `new` operator. In the above example, `Metrics` is a table that contains `MetricsRecord` records.
- You can optionally override the `OnConfiguring` method to change the default configuration or to extend the system with your custom types. For the later see [the section about custom types](#adding-new-types).

The available configuration options are the following:

Example | Description
--- | ---
`.SetTransactionParallelism(8)` | Specify the number of threads working on a single data modification transaction. This should have a high value for console applications and low for servers. Default value: 4
`.SetQueryParallelism(8)` | Specify the number of threads working on a single query. This should have a high value for console applications and low for servers. Default value: 4
`.EnableDurability(false)` | If durability is enabled then instead of overwriting files they are first written to a temporary file, and then swapped with the old one. Disabled by default.
`.SetDatabasePath("/path/to/dir")` | You can configure a custom path to the database folder. By default it is: `{WorkDirectory}/var/data`. Use a relative path to specify a path relative to the working directory.

# Inserting and modifying data

The data is modified in bigger chunks, called "transactions". To create one, just use the `NewTransaction()` method on one of your tables:

```csharp
var db = new DbContext();
var transaction = db.YourTable.NewTransaction();
```

Adding and updating records are both done using the same `Add` method:

```csharp
var record = new MyRecord();
record.Name = "Name1";
record.Time = db.NowUTC;

transaction.Add(record);
```

The `unique` fields determine when two records belong to the same entity. If a record exists already, then it gets updated automatically. You can also remove records using:

```csharp
transaction.Remove(record);
```

At the end you have to commit the transaction to save the changes to disk:

```csharp
foreach(var record in records) {
    transaction.Add(record);
}

transaction.Commit();
```

The bigger the transactions are the higher performance you get. Feel free to store multile gigabytes in a single commit.
If you provide a `true` parameter to the Commit method, then it also forces garbage collection in the .NET assembly at the end:

```csharp
transaction.Commit(true);
```

# Queries

The following are the typical levels that are involved in a query: `database context`, `query`, `cursor` and optionally an `exporter`, when you don't iterate through the records yourself.

```csharp
var db = new DbContext();
var query = db.MyTable.Query()
    .Where(x => x.Name, "John Smith")
    .Where(x => x.Age, 25)
    .OrderByAsc(x => x.LastModified);

var cursor = query.GetCursor();
var exporter = cursor.GetExporter();
exporter.Print();
```

The above example printed the results to the standard output in Linear TSV format, but there's always a shortcut for everything:
```csharp
db.MyTable.Query()
    .Where(x => x.Name, "John Smith")
    .Where(x => x.Age, 25)
    .OrderByAsc(x => x.LastModified)
    .Print();
```

The cursor is an enumerable of your record class, which you can loop through:
```csharp
foreach(var item in cursor) {
    ...
}
```

You can fetch the first item by the `FindOne()` method. The response is `null` if none found:

```csharp
var person = db.MyTable.Query()
    .Where(x => x.Name, "John Smith")
    .Where(x => x.Age, 25)
    .OrderByAsc(x => x.LastModified)
    .FindOne();

if (person != null) {
    ...
}
```

Please find below a complete list of the query directives:

Example directive | Description
--- | ---
`.Where(x => x.Date, "2020-02-09")` | Filtering on a specific value (exact match). This kind of filtering is fast, because it uses the indices.
`.Where(x => x.Date, new LocalDate(2020, 2, 9))` | You can also use the original type of the column in `Where` filters. This also uses the indices.
`.FlexFilter(x => x.Cost > x.Revenue && x.Impressions > 10)` | In flex filters, you can specify an arbitrary expression over the columns. This filtering is slow as it doesn't use the indices.
`.OrderByAsc(x => x.Budget)` `.OrderByDesc(x => x.Budget)` | Ordering by a column in ascending or descending way. You can append multiple sorting directives to sort over multiple fields, in which case the order of the directives is important.
`.Limit(limit, offset)` | The limit value specifies the number of items to return. The offset is the 0-based index of the first item to return. E.g. `.Limit(10, 20)` means to return 10 items, starting from 21th.
`.HintIndexPriority( IndexPriority.Sorting )` | Hinting an index selection algorithm. See the section [Hinting the query planner](#hinting-the-query-planner) for more details.
`.HintIndex("index_name")` | Hinting a specific index. See the section [Hinting the query planner](#hinting-the-query-planner) for more details.

Note that since the cursor is an enumerable of record objects, you can use `Linq expressions` on them. But if you do that, then the whole result set gets loaded into the memory (if there's enough memory for it). Therefore it's recommended to use Linq only in the presence of a `Limit` directive.

# Atomic operations with the OnUpdate event

During the update of a records, there's a narrow window of time, when both the old and the new versions of a record are available in memory, and there's an exclusive lock on the packet of the records. You can facilitate this opportunity by the `OnUpdate` event.

You can specify a `lambda function` as an update event handler on a transaction. The return value of it will be the new version of the record to be stored.

```csharp
var db = new DbContext();
var transaction = db.MyTable.NewTransaction();

transaction.OnUpdate((oldRecord, newRecord) => {
    if (newRecord.Type == MyRecordTypes.NoUpdate) {
        // If you return null, then no changes will be made.
        return null;
    }

    // This incrementation is an atomic change
    newRecord.Counter = oldRecord.Counter + 1;
    
    return newRecord;
});

foreach(var record in records) {
    ...
    transaction.Add(record);
}

transaction.Commit();
```

Note that a lambda function always brings its context with it. Meaning: it can see all variables/fields that are visible inside the method you defined it. This can give great flexibility.

The return value can be of 4 kinds:
- You can return the old record (or a modified version of it), if you would like to minimize the changes.
- You can return the new record (or a modified version of it), if you would like to change the most of the fields.
- You can return `null` in order to avoid any modifications done to the old record. (The new one will just be ignored and not stored anywhere.)
- You can create a completely new record of the same type and return it.

Two things to note:
- Changing the fields of the `unique key` is safe. You won't have any duplicate records, no worries.
- BUT, changing fields which are used in any of the indices defined will result in an exception. It isn't allowed to change the indexed fields inside an `OnUpdate` event, because then the records would need to be relocated into another packet, which cannot be done in an efficient way. (You can do that in application code with the combination of a `remove` and an `add` on a transaction.)

# Making fields unchangeable using OnUpdate

In the previous section we described how to use the OnUpdate event handler in general and specificly for atomic operations.

Let's say that you are importing data from an external server. You would like to insert new records and update the old ones based on the unique key of the data. One of the fields of your schema is the date of creation, called `Created`. You don't want to change that. One solution is (the bad solution) to query the existing records, modify them based on the imported data and persist the result.

But you can do this more efficiently by just pushing all your data into the table (without any previous queries), and doing the fine-tuning inside the update event handler:

```csharp
var db = new DbContext();
var transaction = db.MyTable.NewTransaction();

transaction.OnUpdate((oldRecord, newRecord) => {
    // Keep the creation date always unchanged
    newRecord.Created = oldRecord.Created;
    
    return newRecord;
});

foreach(var record in importedData) {
    ...
    transaction.Add(record);
}

transaction.Commit();
```

# Async support

Asynchronous versions of all methods are available which are involved in input-output operations. Using async is only recommended for server applications. The only case one would use async in a console application is, when there's a source of async events, for example a fast-CGI client, or a hardware interface.

Examples for the query object:
```csharp
await query.FindOneAsync();
await query.PrintAsync();
```

Async iteration over the cursor:
```csharp
while ((var item = await cursor.FetchNextAsync()) != null) {
    ...
}
```

You can also fetch multiple items in one call:
```csharp
List<MyRecord> items = await cursor.FetchAsync(int count);
```

Async methods for the exporter that output the data in `linear TSV` text format:
```csharp
await exporter.PrintAsync();
await exporter.PrintToTsvWriterAsync(TsvWriter output);
await exporter.PrintToStreamAsync(Stream stream);
await exporter.PrintToFileAsync(string path);
```

# Adding new types

With FatCatDB you can use columns of arbitrary types. It's very easy to extend it. The only thing to do is to use the `TypeConverterSetup` parameter in the `OnConfiguring` event of your database context class.

The following example adds the `LocalDateTime` type of the [NodaTime](https://github.com/nodatime/nodatime) library to FatCatDB. (This type is added by default already, this is only an example. See below.)

```csharp
internal class DbContext : DbContextBase {
    public Table<MetricsRecord> Metrics { get; } = new Table<MetricsRecord>();
    
    private LocalDateTimePattern pattern = LocalDateTimePattern.CreateWithInvariantCulture(
        "yyyy-MM-dd HH:mm:ss"
    );

    protected override void OnConfiguring (TypeConverterSetup typeConverterSetup, Configurator configurator) {
        typeConverterSetup
            .RegisterTypeConverter<LocalDateTime, string>((x) => {
                return pattern.Format(x);
            })
            .RegisterTypeConverter<string, LocalDateTime>((x) => {
                return pattern.Parse(x).Value;
            });
    }
}
```

When you add a new type, you alwas have to add 2 converters: one that converts to string, and another that converts back from a string. The 2 template parameters of `RegisterTypeConverter` are the source and the target type. Examples:

```csharp
typeConverterSetup
    .RegisterTypeConverter<MyType, string>((x) => {
        return x.ConvertToString( ... );
    })
    .RegisterTypeConverter<string, MyType>((x) => {
        return new MyType(x);
    });
```

BTW the `LocalDateTime` and `LocalDate` types of [NodaTime](https://github.com/nodatime/nodatime) are added by default to FatCatDB, as this library is the recommended way of dealing with time, instead of the built-in classes of `.NET`.

If you want to sort by your custom type, then it has to implement the `IComperable` interface.

You can also overwrite the built-in converters with your own ones. Just use the same `RegisterTypeConverter` method as in the above examples. You can find the built-in ones in [the constructor of TypeConverterSetup](FatCatDB/TypeConverter.cs).

# Hinting the query planner

The query planner tries to select the best index to execute a query. It has two modes of operation:

- `Filtering priority`: This is the default. Selects the index by first looking at the `Where` statements and just then at the sorting directives. This mode gives the best performance for the queries, but it can happen that sorting is not possible (considering your directives and the indexed fields). In that case you get an error message.
- `Sorting priority`: Let's say for example that you have 10 GBytes of data in a table, and you want to query the 95% of it with a complex sorting on multiple fields. In this case `sorting priority` is the best way to go (performance wise). Use it only when you need the majority of records returned, and you also have sorting directives in your query, which matches an index you defined.

Example:
```csharp
var cursor = db.Metrics.Query()
    .FlexFilter(x => x.Impressions > 10)
    .OrderByAsc(x => x.AccountID)
    .OrderByAsc(x => x.CampaignID)
    .OrderByAsc(x => x.AdID)
    .OrderByDesc(x => x.Date)
    .HintIndexPriority(IndexPriority.Sorting)
    .GetCursor();
```

You can also hint a specific index if you know what you are doing:

```csharp
query.HintIndex("index_name")
```

# ACID and durability

FatCatDB is thread safe, but provides only the `read uncommitted` isolation level for transactions. The primary usage scenario in mind is a single-threaded console application, which loads data (most likely time-series data) from multiple sources, then transforms and stores them before pushing the data to destination endpoints. Application in servers is possible (since `async` methods are provided for everything), but not recommended, because of high memory usage (packet size * concurrency) and the lack of a complete ACID support.

In an average case the schema should be the same as - or at least, it should resemble - the export format. So the data transformation ideally happens during the import and before the storage. This means that `high redundancy` in the schema is normal and expected, in contrary to relational databases.

Durability is provided by two different mechanisms. The first is that the data is stored independently for each index defined. If you define 3 indices for a table, then the data is stored [redundantly 3 times](https://www.youtube.com/watch?v=XmCs-3_DGNE) on the disk in separate folder structures.

The other source of the durability is explicit, and can be enabled by a configuration setting in the [database context](#creating-a-database-context-class) class. If that setting is enabled, then instead of overwriting files, the library first creates temporary ones and then swaps them with the old ones.

# Configurations

Debug build for development:
```bash
$ cd IntegrationTests
$ dotnet build -c Debug
```

Run the integration tests:
```bash
$ cd IntegrationTests
$ dotnet publish -c Release
$ run.sh
```

Create package for NuGet:
```bash
$ cd FatCatDB
$ dotnet build -c Release
$ dotnet pack -c Release
```

# TODO

- Add benchmarks
- Implement tools for data recovery and maintenance
- Extend the integration tests
- Implement unit tests after the interfaces are finalized
- Implement aggregation functionality
- Implement `left join` and `inner join`
- Implement the text description of the query plan.
