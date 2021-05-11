# PostgreSQL R2DBC Driver [![Java CI with Maven](https://github.com/pgjdbc/r2dbc-postgresql/workflows/Java%20CI%20with%20Maven/badge.svg?branch=main)](https://github.com/pgjdbc/r2dbc-postgresql/actions?query=workflow%3A%22Java+CI+with+Maven%22+branch%3Amain) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.r2dbc/r2dbc-postgresql/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.r2dbc/r2dbc-postgresql)

This project contains the [PostgreSQL][p] implementation of the [R2DBC SPI][r].  This implementation is not intended to be used directly, but rather to be used as the backing implementation for a humane client library to delegate to.

This driver provides the following features:

* Implements R2DBC 0.9
* Login with username/password (MD5, SASL/SCRAM) or implicit trust
* SCRAM authentication
* Unix Domain Socket transport
* TLS
* Explicit transactions
* Notifications
* Logical Decode
* Binary data transfer
* Execution of prepared statements with bindings
* Execution of batch statements without bindings
* Read and write support for all data types except LOB types (e.g. `BLOB`, `CLOB`)
* Fetching of `REFCURSOR` using `io.r2dbc.postgresql.api.RefCursor`
* Extension points to register `Codec`s to handle additional PostgreSQL data types

Next steps:

* Multi-dimensional arrays

[p]: https://www.postgresql.org
[r]: https://github.com/r2dbc/r2dbc-spi

## Code of Conduct

This project is governed by the [Code of Conduct](.github/CODE_OF_CONDUCT.adoc). By participating, you are expected to uphold this code of conduct. Please report unacceptable behavior to [r2dbc@googlegroups.com](mailto:r2dbc@googlegroups.com).

## Getting Started

Here is a quick teaser of how to use R2DBC PostgreSQL in Java:

**URL Connection Factory Discovery**

```java
ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:postgresql://<host>:5432/<database>");

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
```

**Programmatic Connection Factory Discovery**

```java
Map<String, String> options = new HashMap<>();
options.put("lock_timeout", "10s");
options.put("statement_timeout", "5m");

ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
   .option(DRIVER, "postgresql")
   .option(HOST, "...")
   .option(PORT, 5432)  // optional, defaults to 5432
   .option(USER, "...")
   .option(PASSWORD, "...")
   .option(DATABASE, "...")  // optional
   .option(OPTIONS, options) // optional
   .build());

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();

// Alternative: Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

**Supported ConnectionFactory Discovery Options**

| Option            | Description
| ----------------- | -----------
| `ssl`             | Enables SSL usage (`SSLMode.VERIFY_FULL`).
| `driver`          | Must be `postgresql`.
| `host`            | Server hostname to connect to.
| `port`            | Server port to connect to.  Defaults to `5432`. _(Optional)_
| `socket`          | Unix Domain Socket path to connect to as alternative to TCP. _(Optional)_
| `username`        | Login username.
| `password`        | Login password. _(Optional when using TLS Certificate authentication)_
| `database`        | Database to select. _(Optional)_
| `applicationName` | The name of the application connecting to the database.  Defaults to `r2dbc-postgresql`. _(Optional)_
| `autodetectExtensions` | Whether to auto-detect and register `Extension`s from the class path.  Defaults to `true`. _(Optional)_
| `compatibilityMode` | Enable compatibility mode for cursored fetching. Required when using newer pgpool versions. Defaults to `false`. _(Optional)_
| `errorResponseLogLevel` | Log level for error responses. Any of `OFF`, `DEBUG`, `INFO`, `WARN` or `ERROR`  Defaults to `DEBUG`. _(Optional)_
| `extensions`      | Collection of `Extension` to provide additional extensions when creating a connection factory. Defaults to empty. _(Optional)_
| `fetchSize`       | The default number of rows to return when fetching results. Defaults to `0` for unlimited. _(Optional)_
| `forceBinary`     | Whether to force binary transfer. Defaults to `false`. _(Optional)_
| `loopResources`   | TCP/Socket LoopResources (depends on the endpoint connection type). _(Optional)_
| `lockWaitTimeout` | Lock wait timeout. _(Optional)_
| `noticeLogLevel`  | Log level for error responses. Any of `OFF`, `DEBUG`, `INFO`, `WARN` or `ERROR`  Defaults to `DEBUG`. _(Optional)_
| `preferAttachedBuffers` |Configure whether codecs should prefer attached data buffers. The default is `false`, meaning that codecs will copy data from the input buffer into a byte array. Enabling attached buffers requires consumption of values such as `Json`  to avoid memory leaks.
| `preparedStatementCacheQueries` | Determine the number of queries that are cached in each connection. The default is `-1`, meaning there's no limit. The value of `0` disables the cache. Any other value specifies the cache size.
| `options`         | A `Map<String, String>` of connection parameters. These are applied to each database connection created by the `ConnectionFactory`. Useful for setting generic [PostgreSQL connection parameters][psql-runtime-config]. _(
Optional)_
| `schema`          | The search path to set. _(Optional)_
| `sslMode`         | SSL mode to use, see `SSLMode` enum. Supported values: `DISABLE`, `ALLOW`, `PREFER`, `REQUIRE`, `VERIFY_CA`, `VERIFY_FULL`, `TUNNEL`. _(Optional)_
| `sslRootCert`     | Path to SSL CA certificate in PEM format. Can be also a resource path. _(Optional)_
| `sslKey`          | Path to SSL key for TLS authentication in PEM format. Can be also a resource path. _(Optional)_
| `sslCert`         | Path to SSL certificate for TLS authentication in PEM format. Can be also a resource path. _(Optional)_
| `sslPassword`     | Key password to decrypt SSL key. _(Optional)_
| `sslHostnameVerifier` | `javax.net.ssl.HostnameVerifier` implementation. _(Optional)_
| `statementTimeout`| Statement timeout. _(Optional)_
| `tcpNoDelay`      | Enable/disable TCP NoDelay. Enabled by default. _(Optional)_
| `tcpKeepAlive`    | Enable/disable TCP KeepAlive. Disabled by default. _(Optional)_

**Programmatic Configuration**

```java
Map<String, String> options = new HashMap<>();
options.put("lock_timeout", "10s");

PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
    .host("...")
    .port(5432)  // optional, defaults to 5432
    .username("...")
    .password("...")
    .database("...")  // optional
    .options(options) // optional
    .build());

Mono<Connection> mono = connectionFactory.create();
```

PostgreSQL uses index parameters that are prefixed with `$`.  The following SQL statement makes use of parameters:

```sql
INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3)
```

Parameters are referenced using the same identifiers when binding these:

```java
mono.flatMapMany(connection -> connection
                .createStatement("INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3)")
                .bind("$1", 1)
                .bind("$2", "Walter")
                .bind("$3", "White")
                .execute());
```

Binding also allowed positional index (zero-based) references.  The parameter index is derived from the parameter discovery order when parsing the query.

### Maven configuration

Artifacts can be found on [Maven Central](https://search.maven.org/search?q=r2dbc-postgresql).

```xml
<dependency>
  <groupId>org.postgresql</groupId>
  <artifactId>r2dbc-postgresql</artifactId>
  <version>${version}</version>
</dependency>
```

If you'd rather like the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

```xml
<dependency>
  <groupId>org.postgresql</groupId>
  <artifactId>r2dbc-postgresql</artifactId>
  <version>${version}.BUILD-SNAPSHOT</version>
</dependency>

<repository>
<id>sonatype-nexus-snapshots</id>
<name>Sonatype OSS Snapshot Repository</name>
<url>https://oss.sonatype.org/content/repositories/snapshots</url>
</repository>
```

## Cursors

R2DBC Postgres supports both, the [simple](https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4)
and [extended](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY) message flow.

Cursored fetching is activated by configuring a `fetchSize`. Postgres cursors are valid for the duration of a transaction. R2DBC can use cursors in auto-commit mode (`Execute` and `Flush`) to not
require an explicit transaction (`BEGIN…COMMIT/ROLLBACK`). Newer pgpool versions don't support this feature. To work around this limitation, either use explicit transactions when configuring a fetch
size or enable compatibility mode. Compatibility mode avoids cursors in auto-commit mode (`Execute` with no limit + `Sync`). Cursors in a transaction use `Execute` (with fetch size as limit) + `Sync`
as message flow.

## Listen/Notify

Listen and Notify provide a simple form of signal or inter-process communication mechanism for processes accessing the same PostgreSQL database. For Listen/Notify, two actors are involved: The
sender (notify) and the receiver (listen). The following example uses two connections to illustrate how they work together:

```java
PostgresqlConnection sender= …;
        PostgresqlConnection receiver= …;

Flux<Notification> listen = receiver.createStatement("LISTEN mymessage")
                                .execute()
                                .flatMap(PostgresqlResult::getRowsUpdated)
        .thenMany(receiver.getNotifications());

        Mono<Void> notify=sender.createStatement("NOTIFY mymessage, 'Hello World'")
        .execute()
        .flatMap(PostgresqlResult::getRowsUpdated)
        .then();
```                                                                                                                       

Upon subscription, the first connection enters listen mode and publishes incoming `Notification`s as `Flux`. The second connection broadcasts a notification to the `mymessage` channel upon
subscription.

## Transaction Definitions

Postgres supports additional options when starting a transaction. In particular, the following options can be specified:

* Isolation Level (`isolationLevel`) (reset after the transaction to previous value)
* Transaction Mutability (`readOnly`)
* Deferrable Mode (`deferrable`)

These options can be specified upon transaction begin to start the transaction and apply options in a single command roundtrip:

```java
PostgresqlConnection connection= …;

        connection.beginTransaction(PostgresTransactionDefinition.from(IsolationLevel.SERIALIZABLE).readOnly().notDeferrable());
```

See also: https://www.postgresql.org/docs/current/sql-begin.html

## JSON/JSONB support

PostgreSQL supports JSON by storing values in `JSON`/`JSONB` columns. These values can be consumed and written using the regular R2DBC SPI and by using driver-specific extensions with
the `io.r2dbc.postgresql.codec.Json` type.

You can choose from two approaches:

* Native JSONB encoding using the `Json` wrapper type.
* Using scalar types.

The difference between the `Json` type and scalar types is that `Json` values are written encoded as `JSONB` to the database.
`byte[]` and `String` types are represented as `BYTEA` respective `VARCHAR` and require casting (`$1::JSON`) when used with parameterized statements.

The following code shows `INSERT` and `SELECT` cases for JSON interaction:

```sql
CREATE TABLE my_table (my_json JSON);
```

**Write JSON**

```java
connection.createStatement("INSERT INTO my_table (my_json) VALUES($1)")
            .bind("$1", Json.of("{\"hello\": \"world\"}")).execute();
```

**Consume JSON**

```java
connection.createStatement("SELECT my_json FROM my_table")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_json", Json.class)))
            .map(Json::asString);
```

**Write JSON using casting**

```java
connection.createStatement("INSERT INTO my_table (my_json) VALUES($1::JSON)")
    .bind("$1", "{\"hello\": \"world\"}").execute();
```

**Consume JSON as scalar type**

```java
connection.createStatement("SELECT my_json FROM my_table")
    .execute()
    .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_json", String.class)));
```

The following types are supported for JSON exchange:

* `io.r2dbc.postgresql.codec.Json`
* `ByteBuf` (must be released after usage to avoid memory leaks)
* `ByteBuffer`
* `byte[]`
* `String`
* `InputStream` (must be closed after usage to avoid memory leaks)

## Cursors

The driver can consume cursors that were created by PL/pgSQL as `refcursor`. 
Cursors are represented as `RefCursor` objects. Cursors obtained from `Result` can be used to fetch the cursor directly. 
Since cursors are stateful, they must be closed once they are no longer in use.

```java
connection.createStatement("SELECT show_cities_multiple()").execute()
    .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, RefCursor.class)))
    .flatMap(cursor -> {
        Mono<PostgresResult> data = cursor.fetch()
            .flatMap(…)
            .then(rc.close());
        return data;
    });

```

## Logical Decode

PostgreSQL allows replication streaming and decoding persistent changes to a database's tables into useful chunks of data.
In PostgreSQL, logical decoding is implemented by decoding the contents of the write-ahead log, which describe changes on a storage level, into an application-specific form such as a stream of tuples or SQL statements.

Consuming the replication stream is a four-step process:

1. Obtain a replication connection via `PostgresqlConnectionFactory.replication()`.
2. Create a replication slot (physical/logical).
3. Initiate replication using the replication slot.
4. Once the replication stream is set up, you can consume and map the binary data using `ReplicationStream.map(…)`.

On application shutdown, `close()` the `ReplicationStream`.

Note that a connection is busy once the replication is active and a connection can have at most one active replication stream.  

```java
Mono<PostgresqlReplicationConnection> replicationMono = connectionFactory.replication();

// later:
ReplicationSlotRequest request = ReplicationSlotRequest.logical()
                                        .slotName("my_slot")
                                        .outputPlugin("test_decoding")
                                        .temporary()
                                        .build();
Mono<ReplicationSlot> createSlot = replicationConnection.createSlot(request);

ReplicationRequest replicationRequest = ReplicationRequest.logical()
                                        .slotName("my_slot")
                                        .startPosition(LogSequenceNumber.valueOf(0))
                                        .slotOption("skip-empty-xacts", true)
                                        .slotOption("include-xids", false)
                                        .build();

Flux<T> replicationStream = replicationConnection.startReplication(replicationRequest).flatMapMany(it -> {
    return it.map(byteBuf -> {…})
        .doOnError(t -> it.close().subscribe());
});
```

## Postgres Enum Types

Applications may make use of Postgres enumerated types by using `EnumCodec` to map custom types to Java `enum` types. 
`EnumCodec` requires the Postgres OID and the Java to map enum values to the Postgres protocol and to materialize Enum instances from Postgres results. 
You can configure a `CodecRegistrar` through `EnumCodec.builder()` for one or more enumeration type mappings. Make sure to use different Java enum types otherwise the driver is not able to distinguish between Postgres OIDs. 

Example:

**SQL:**

```sql
CREATE TYPE my_enum AS ENUM ('FIRST', 'SECOND');
``` 

**Java Model:**

```java
enum MyEnumType {
  FIRST, SECOND;
}
```

**Codec Registration:**

```java
PostgresqlConnectionConfiguration.builder()
        .codecRegistrar(EnumCodec.builder().withEnum("my_enum",MyEnumType.class).build());
```

When available, the driver registers also an array variant of the codec.

## Data Type Mapping

This reference table shows the type mapping between [PostgreSQL][p] and Java data types:

| PostgreSQL Type                                 | Supported Data Type                                                                                                                           | 
|:------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------|
| [`bigint`][psql-bigint-ref]                     | [**`Long`**][java-long-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref] |
| [`bit`][psql-bit-ref]                           | Not yet supported.|
| [`bit varying`][psql-bit-ref]                   | Not yet supported.|
| [`boolean or bool`][psql-boolean-ref]           | [`Boolean`][java-boolean-ref]|
| [`box`][psql-box-ref]                           | **`Box`**|
| [`bytea`][psql-bytea-ref]                       | [**`ByteBuffer`**][java-ByteBuffer-ref], [`byte[]`][java-byte-ref], [`Blob`][r2dbc-blob-ref]|
| [`character`][psql-character-ref]               | [`String`][java-string-ref]|
| [`character varying`][psql-character-ref]       | [`String`][java-string-ref]|
| [`cidr`][psql-cidr-ref]                         | Not yet supported.|
| [`circle`][psql-circle-ref]                     | **`Circle`**|
| [`date`][psql-date-ref]                         | [`LocalDate`][java-ld-ref]|
| [`double precision`][psql-floating-point-ref]   | [**`Double`**][java-double-ref], [`Float`][java-float-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref], [`Long`][java-long-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref]|
| [enumerated types][psql-enum-ref]               | Client code `Enum` types through `EnumCodec`|
| [`hstore`][psql-hstore-ref]                     | [**`Map`**][java-map-ref]|
| [`inet`][psql-inet-ref]                         | [**`InetAddress`**][java-inet-ref]|
| [`integer`][psql-integer-ref]                   | [**`Integer`**][java-integer-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Long`][java-long-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref]|
| [`interval`][psql-interval-ref]                 | **`Interval`**|
| [`json`][psql-json-ref]                         | **`Json`**, [`String`][java-string-ref]. Reading: `ByteBuf`[`byte[]`][java-primitive-ref], [`ByteBuffer`][java-ByteBuffer-ref], [`String`][java-string-ref], [`InputStream`][java-inputstream-ref]|
| [`jsonb`][psql-json-ref]                        | **`Json`**, [`String`][java-string-ref]. Reading: `ByteBuf`[`byte[]`][java-primitive-ref], [`ByteBuffer`][java-ByteBuffer-ref], [`String`][java-string-ref], [`InputStream`][java-inputstream-ref]|
| [`line`][psql-line-ref]                         | **`Line`**|
| [`lseg`][psql-lseq-ref]                         | **`Lseg`**|
| [`macaddr`][psql-macaddr-ref]                   | Not yet supported.|
| [`macaddr8`][psql-macaddr8-ref]                 | Not yet supported.|
| [`money`][psql-money-ref]                       | Not yet supported. Please don't use this type. It is a very poor implementation. |
| [`name`][psql-name-ref]                         | [**`String`**][java-string-ref]
| [`numeric`][psql-bignumeric-ref]                | [`BigDecimal`][java-bigdecimal-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref], [`Long`][java-long-ref], [`BigInteger`][java-biginteger-ref]|
| [`oid`][psql-oid-ref]                           | [**`Integer`**][java-integer-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Long`][java-long-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref]|
| [`path`][psql-path-ref]                         | **`Path`**|
| [`pg_lsn`][psql-pg_lsn-ref]                     | Not yet supported.|
| [`point`][psql-point-ref]                       | **`Point`**|
| [`polygon`][psql-polygon-ref]                   | **`Polygon`**|
| [`real`][psql-real-ref]                         | [**`Float`**][java-float-ref], [`Double`][java-double-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref], [`Long`][java-long-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref]|
| [`smallint`][psql-smallint-ref]                 | [**`Short`**][java-short-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Integer`][java-integer-ref], [`Long`][java-long-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref]|
| [`smallserial`][psql-smallserial-ref]           | [**`Integer`**][java-integer-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Long`][java-long-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref]|
| [`serial`][psql-serial-ref]                     | [**`Long`**][java-long-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref]|
| [`text`][psql-text-ref]                         | [**`String`**][java-string-ref], [`Clob`][r2dbc-clob-ref]|
| [`time [without time zone]`][psql-time-ref]     | [`LocalTime`][java-lt-ref]|
| [`time [with time zone]`][psql-time-ref]        | [`OffsetTime`][java-ot-ref]|
| [`timestamp [without time zone]`][psql-time-ref]|[**`LocalDateTime`**][java-ldt-ref], [`LocalTime`][java-lt-ref], [`LocalDate`][java-ld-ref], [`java.util.Date`][java-legacy-date-ref]|
| [`timestamp [with time zone]`][psql-time-ref]   | [**`OffsetDatetime`**][java-odt-ref], [`ZonedDateTime`][java-zdt-ref], [`Instant`][java-instant-ref]|
| [`tsquery`][psql-tsquery-ref]                   | Not yet supported.|
| [`tsvector`][psql-tsvector-ref]                 | Not yet supported.|
| [`txid_snapshot`][psql-txid_snapshot-ref]       | Not yet supported.|
| [`uuid`][psql-uuid-ref]                         | [**`UUID`**][java-uuid-ref], [`String`][java-string-ref]||
| [`xml`][psql-xml-ref]                           | Not yet supported. |

Types in **bold** indicate the native (default) Java type.

Support for the following single-dimensional arrays (read and write):

| PostgreSQL Type                                 | Supported Data Type                   |
|:------------------------------------------------|:--------------------------------------|
| [`bytea[]`][psql-bytea-ref]                     | [**`ByteBuffer[]`**][java-ByteBuffer-ref], [`byte[][]`][java-byte-ref]|
| [`boolean[] or bool[]`][psql-boolean-ref]       | [`Boolean[]`][java-boolean-ref]       |
| [`box[]`][psql-box-ref]                         | **`Box[]`**|
| [`character`][psql-character-ref]               | [`String[]`][java-string-ref]|
| [`character varying`][psql-character-ref]       | [`String[]`][java-string-ref]|
| [`circle[]`][psql-circle-ref]                   | **`Circle[]`**|
| [`date[]`][psql-date-ref]                       | [`LocalDate[]`][java-ld-ref]|
| [`double precision[]`][psql-floating-point-ref] | [**`Double[]`**][java-double-ref], [`Float[]`][java-float-ref], [`Boolean[]`][java-boolean-ref], [`Byte[]`][java-byte-ref], [`Short[]`][java-short-ref], [`Integer[]`][java-integer-ref], [`Long[]`][java-long-ref], [`BigDecimal[]`][java-bigdecimal-ref], [`BigInteger[]`][java-biginteger-ref]|
| [enumerated type arrays][psql-enum-ref]         | Client code `Enum[]` types through `EnumCodec`|
| [`inet[]`][psql-inet-ref]                       | [**`InetAddress[]`**][java-inet-ref]|
| [`integer`[]][psql-integer-ref]                 | [**`Integer[]`**][java-integer-ref], [`Boolean[]`][java-boolean-ref], [`Byte[]`][java-byte-ref], [`Short[]`][java-short-ref], [`Long[]`][java-long-ref], [`BigDecimal[]`][java-bigdecimal-ref], [`BigInteger[]`][java-biginteger-ref]|
| [`interval[]`][psql-interval-ref]               | **`Interval[]`**|
| [`line[]`][psql-line-ref]                       | **`Line[]`**|
| [`numeric[]`][psql-bignumeric-ref]              | [`BigDecimal[]`][java-bigdecimal-ref], [`Boolean[]`][java-boolean-ref], [`Byte[]`][java-byte-ref], [`Short[]`][java-short-ref], [`Integer[]`][java-integer-ref], [`Long[]`][java-long-ref], [`BigInteger[]`][java-biginteger-ref]|
| [`double precision[]`][psql-floating-point-ref] | [`Double[]`][java-double-ref]         |
| [`point[]`][psql-point-ref]                     | **`Point[]`**|
| [`polygon[]`][psql-polygon-ref]                 | **`Polygon[]`**|
| [`real[]`][psql-real-ref]                       | [**`Float[]`**][java-float-ref], [`Double[]`][java-double-ref], [`Boolean[]`][java-boolean-ref], [`Byte[]`][java-byte-ref], [`Short[]`][java-short-ref], [`Integer[]`][java-integer-ref], [`Long[]`][java-long-ref], [`BigDecimal[]`][java-bigdecimal-ref], [`BigInteger[]`][java-biginteger-ref]|
| [`smallint[]`][psql-smallint-ref]               | [**`Short[]`**][java-short-ref], [`Boolean[]`][java-boolean-ref], [`Byte[]`][java-byte-ref], [`Integer[]`][java-integer-ref], [`Long[]`][java-long-ref], [`BigDecimal[]`][java-bigdecimal-ref], [`BigInteger[]`][java-biginteger-ref]|
| [`smallserial[]`][psql-smallserial-ref]         | [**`Integer[]`**][java-integer-ref], [`Boolean[]`][java-boolean-ref], [`Byte[]`][java-byte-ref], [`Short[]`][java-short-ref], [`Long[]`][java-long-ref], [`BigDecimal[]`][java-bigdecimal-ref], [`BigInteger[]`][java-biginteger-ref]|
| [`serial[]`][psql-serial-ref]                   | [**`Long[]`**][java-long-ref], [`Boolean[]`][java-boolean-ref], [`Byte[]`][java-byte-ref], [`Short[]`][java-short-ref], [`Integer[]`][java-integer-ref], [`BigDecimal[]`][java-bigdecimal-ref], [`BigInteger[]`][java-biginteger-ref]|
| [`text[]`][psql-text-ref]                       | [`String[]`][java-string-ref]         |
| [`time[] [without time zone]`][psql-time-ref]   | [`LocalTime[]`][java-lt-ref]|
| [`time[] [with time zone]`][psql-time-ref]      | [`OffsetTime[]`][java-ot-ref]|
| [`timestamp[] [without time zone]`][psql-time-ref]|[**`LocalDateTime[]`**][java-ldt-ref], [`LocalTime[]`][java-lt-ref], [`LocalDate[]`][java-ld-ref], [`java.util.Date[]`][java-legacy-date-ref]|
| [`timestamp[] [with time zone]`][psql-time-ref] | [**`OffsetDatetime[]`**][java-odt-ref], [`ZonedDateTime[]`][java-zdt-ref], [`Instant[]`][java-instant-ref]|
| [`uuid[]`][psql-uuid-ref]                       | [**`UUID[]`**][java-uuid-ref], [`String[]`][java-string-ref]||

[psql-bigint-ref]: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT
[psql-bit-ref]: https://www.postgresql.org/docs/current/datatype-numeric.html
[psql-boolean-ref]: https://www.postgresql.org/docs/current/datatype-boolean.html
[psql-box-ref]: https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.8
[psql-bytea-ref]: https://www.postgresql.org/docs/current/datatype-binary.html#id-1.5.7.12.9
[psql-character-ref]: https://www.postgresql.org/docs/current/datatype-character.html
[psql-cidr-ref]: https://www.postgresql.org/docs/current/datatype-net-types.html#DATATYPE-CIDR
[psql-circle-ref]: https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-CIRCLE
[psql-date-ref]: https://www.postgresql.org/docs/current/datatype-datetime.html
[psql-floating-point-ref]: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-FLOAT
[psql-enum-ref]: https://www.postgresql.org/docs/current/datatype-enum.html
[psql-hstore-ref]: https://www.postgresql.org/docs/current/hstore.html
[psql-inet-ref]: https://www.postgresql.org/docs/current/datatype-net-types.html#DATATYPE-INET
[psql-integer-ref]: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT
[psql-interval-ref]: https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT
[psql-json-ref]: https://www.postgresql.org/docs/current/datatype-json.html
[psql-line-ref]: https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LINE
[psql-lseq-ref]: https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LINE
[psql-bignumeric-ref]: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
[psql-macaddr-ref]: https://www.postgresql.org/docs/current/datatype-net-types.html#DATATYPE-MACADDR
[psql-macaddr8-ref]: https://www.postgresql.org/docs/current/datatype-net-types.html#DATATYPE-MACADDR8
[psql-name-ref]: https://www.postgresql.org/docs/13/datatype-character.html#DATATYPE-CHARACTER-SPECIAL-TABLE
[psql-money-ref]: https://www.postgresql.org/docs/current/datatype.html
[psql-oid-ref]: https://www.postgresql.org/docs/current/datatype-oid.html
[psql-path-ref]: https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.9
[psql-pg_lsn-ref]: https://www.postgresql.org/docs/current/datatype-pg-lsn.html
[psql-point-ref]: https://www.postgresql.org/docs/current/datatype-geometric.html#id-1.5.7.16.5
[psql-polygon-ref]: https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-POLYGON
[psql-real-ref]: https://www.postgresql.org/docs/current/datatype.html
[psql-smallint-ref]: https://www.postgresql.org/docs/current/datatype.html
[psql-smallserial-ref]: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL
[psql-serial-ref]: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL
[psql-text-ref]: https://www.postgresql.org/docs/current/datatype.html
[psql-time-ref]: https://www.postgresql.org/docs/current/datatype-datetime.html
[psql-tsquery-ref]: https://www.postgresql.org/docs/current/datatype-textsearch.html#DATATYPE-TSQUERY
[psql-tsvector-ref]: https://www.postgresql.org/docs/current/datatype-textsearch.html#DATATYPE-TSVECTOR
[psql-txid_snapshot-ref]: https://www.postgresql.org/docs/current/datatype.html
[psql-uuid-ref]: https://www.postgresql.org/docs/current/datatype-uuid.html
[psql-xml-ref]: https://www.postgresql.org/docs/current/datatype-xml.html
[psql-runtime-config]: https://www.postgresql.org/docs/current/runtime-config-client.html

[r2dbc-blob-ref]: https://r2dbc.io/spec/0.9.0.M1/api/io/r2dbc/spi/Blob.html
[r2dbc-clob-ref]: https://r2dbc.io/spec/0.9.0.M1/api/io/r2dbc/spi/Clob.html

[java-bigdecimal-ref]: https://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html
[java-biginteger-ref]: https://docs.oracle.com/javase/8/docs/api/java/math/BigInteger.html
[java-boolean-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Boolean.html
[java-byte-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Byte.html
[java-ByteBuffer-ref]: https://docs.oracle.com/javase/8/docs/api/java/nio/ByteBuffer.html
[java-double-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Double.html
[java-float-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Float.html
[java-map-ref]: https://docs.oracle.com/javase/8/docs/api/java/util/Map.html
[java-inet-ref]: https://docs.oracle.com/javase/8/docs/api/java/net/InetAddress.html
[java-inputstream-ref]: https://docs.oracle.com/javase/8/docs/api/java/io/InputStream.html
[java-instant-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/Instant.html
[java-integer-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Integer.html
[java-long-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Long.html
[java-ldt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html
[java-ld-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDate.html
[java-lt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalTime.html
[java-legacy-date-ref]: https://docs.oracle.com/javase/8/docs/api/java/util/Date.html
[java-odt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/OffsetDateTime.html
[java-ot-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/OffsetTime.html
[java-primitive-ref]: https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
[java-short-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Short.html
[java-string-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/String.html
[java-uuid-ref]: https://docs.oracle.com/javase/8/docs/api/java/util/UUID.html
[java-zdt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html

## Extension mechanism
This driver accepts the following extensions:

* `CodecRegistrar` to contribute `Codec`s for PostgreSQL ObjectIDs. 

Extensions can be registered programmatically using `PostgresConnectionConfiguration` or discovered using Java's `ServiceLoader` mechanism (from `META-INF/services/io.r2dbc.postgresql.extension.Extension`).

The driver ships with built-in dynamic codecs (e.g. `hstore`) that are registered during the connection handshake depending on their availability while connecting. Note that Postgres extensions registered after a connection was established require a reconnect to initialize the codec. 

## Logging
If SL4J is on the classpath, it will be used. Otherwise, there are two possible fallbacks: Console or `java.util.logging.Logger`). By default, the Console fallback is used. To use the JDK loggers, set the `reactor.logging.fallback` System property to `JDK`.

Logging facilities:

* Driver Logging (`io.r2dbc.postgresql`)
* Query Logging (`io.r2dbc.postgresql.QUERY` on `DEBUG` level)
* Parameters' values Logging (`io.r2dbc.postgresql.PARAM` on `DEBUG` level)
* Transport Logging (`io.r2dbc.postgresql.client`)
    * `DEBUG` enables `Message` exchange logging
    * `TRACE` enables traffic logging
    
Logging that is associated with a connection reports the logical connection id (`cid`) which is a driver-local connection counter and the Postgres Process Id (`pid`) once the connection handshake finishes.
    
## Getting Help

Having trouble with R2DBC? We'd love to help!

* Check the [spec documentation](https://r2dbc.io/spec/0.9.0.M1/spec/html/), and [Javadoc](https://r2dbc.io/spec/0.9.0.M1/api/).
* If you are upgrading, check out the [changelog](https://r2dbc.io/spec/0.9.0.M1/CHANGELOG.txt) for "new and noteworthy" features.
* Ask a question - we monitor [stackoverflow.com](https://stackoverflow.com) for questions
  tagged with [`r2dbc`](https://stackoverflow.com/tags/r2dbc). 
  You can also chat with the community on [Gitter](https://gitter.im/r2dbc/r2dbc).
* Report bugs with R2DBC PostgreSQL at [github.com/pgjdbc/r2dbc-postgresql/issues](https://github.com/pgjdbc/r2dbc-postgresql/issues).

## Reporting Issues

R2DBC uses GitHub as issue tracking system to record bugs and feature requests. 
If you want to raise an issue, please follow the recommendations below:

* Before you log a bug, please search the [issue tracker](https://github.com/pgjdbc/r2dbc-postgresql/issues) to see if someone has already reported the problem.
* If the issue doesn't already exist, [create a new issue](https://github.com/pgjdbc/r2dbc-postgresql/issues/new).
* Please provide as much information as possible with the issue report, we like to know the version of R2DBC PostgreSQL that you are using and JVM version.
* If you need to paste code, or include a stack trace use Markdown ``` escapes before and after your text.
* If possible try to create a test-case or project that replicates the issue. 
Attach a link to your code or a compressed file containing your code.

## Building from Source

You don't need to build from source to use R2DBC PostgreSQL (binaries in Maven Central), but if you want to try out the latest and greatest, R2DBC PostgreSQL can be easily built with the
[maven wrapper](https://github.com/takari/maven-wrapper). You also need JDK 1.8 and Docker to run integration tests.

```bash
 $ ./mvnw clean install
```

If you want to build with the regular `mvn` command, you will need [Maven v3.5.0 or above](https://maven.apache.org/run-maven/index.html).

_Also see [CONTRIBUTING.adoc](.github/CONTRIBUTING.adoc) if you wish to submit pull requests._

### Running JMH Benchmarks

Running the JMH benchmarks builds and runs the benchmarks without running tests.

```bash
 $ ./mvnw clean install -Pjmh
```

## License
This project is released under version 2.0 of the [Apache License][l].

[l]: https://www.apache.org/licenses/LICENSE-2.0
