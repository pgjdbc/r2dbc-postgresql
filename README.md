# Reactive Relational Database Connectivity PostgreSQL Implementation

This project contains the [PostgreSQL][p] implementation of the [R2DBC SPI][r].  This implementation is not intended to be used directly, but rather to be used as the backing implementation for a humane client library to delegate to.

This driver provides the following features:

* Login with username/password or implicit trust
* Explict transactions
* Execution of prepared statements with bindings
* Execution of batch statements without bindings
* Read and write support for all data types except LOB types (e.g. `BLOB`, `CLOB`)

Next steps:

* TLS
* Multi-dimensional arrays
* Notifications
* SCRAM authentication
* Binary data transfer

[p]: https://www.postgresql.org
[r]: https://github.com/r2dbc/r2dbc-spi

## Maven
Both milestone and snapshot artifacts (library, source, and javadoc) can be found in Maven repositories.

```xml
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-postgresql</artifactId>
  <version>1.0.0.M7</version>
</dependency>
```

Artifacts can be found at the following repositories.

### Repositories
```xml
<repository>
    <id>spring-snapshots</id>
    <name>Spring Snapshots</name>
    <url>https://repo.spring.io/snapshot</url>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
```

```xml
<repository>
    <id>spring-milestones</id>
    <name>Spring Milestones</name>
    <url>https://repo.spring.io/milestone</url>
    <snapshots>
        <enabled>false</enabled>
    </snapshots>
</repository>
```

## Usage
Configuration of the `ConnectionFactory` can be accomplished in two ways:

### Connection Factory Discovery
```java
ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
   .option(DRIVER, "postgresql")
   .option(HOST, "...")
   .option(PORT, "...")  // optional, defaults to 5432
   .option(USER, "...")
   .option(PASSWORD, "...")
   .option(DATABASE, "...")  // optional
   .build());

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();

// Alternative: Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

Supported Connection Factory Discovery options:

| Option | Description
| ------ | -----------
| `driver` | Must be `postgresql`
| `host` | Server hostname to connect to
| `port` | Server port to connect to.  Defaults to 5432. _(Optional)_
| `username` | Login username
| `password` | Login password
| `database` | Database to select. _(Optional)_
| `applicationName` | The name of the application connecting to the database.  Defaults to `r2dbc-postgresql`. _(Optional)_
| `schema` | The schema to set. _(Optional)_

### Programmatic
```java
ConnectionFactory connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
    .host("...")
    .port("...").  // optional, defaults to 5432
    .username("...")
    .password("...")
    .database("...")  // optional
    .build());

Mono<Connection> mono = connectionFactory.create();
```

PostgreSQL uses index parameters that are prefixed with `$`.  The following SQL statement makes use of parameters:

```sql
INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3)
```
Parameters are referenced using the same identifiers when binding these:

```java
mono.subscribe(connection -> {
            connection.beginTransaction();
            connection
                    .createStatement("INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3)")
                    .bind("$1", 1)
                    .bind("$2", "Walter")
                    .bind("$3", "White")
                    .execute();
            connection.commitTransaction();
        });
```

Binding also allowed positional index (zero-based) references.  The parameter index is derived from the parameter discovery order when parsing the query.


## Data Type Mapping

This reference table shows the type mapping between [PostgreSQL][p] and Java data types:

| PostgreSQL Type                                 | Supported Data Type                                                                                                                           | 
|:------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------|
| [`bigint`][psql-bigint-ref]                     | [**`Long`**][java-long-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref] |
| [`bit`][psql-bit-ref]                           | Not yet supported.|
| [`bit varying`][psql-bit-ref]                   | Not yet supported.|
| [`boolean or bool`][psql-boolean-ref]           | [`Boolean`][java-boolean-ref]|
| [`box`][psql-box-ref]                           | Not yet supported.|
| [`bytea`][psql-bytea-ref]                       | Not yet supported.|
| [`character`][psql-character-ref]               | [`String`][java-string-ref]|
| [`character varying`][psql-character-ref]       | [`String`][java-string-ref]|
| [`cidr`][psql-cidr-ref]                         | Not yet supported.|
| [`circle`][psql-circle-ref]                     | Not yet supported.|
| [`date`][psql-date-ref]                         | [`LocalDate`][java-ld-ref]|
| [`double precision`][psql-floating-point-ref]   | [**`Double`**][java-double-ref], [`Float`][java-float-ref]|
| [`inet`][psql-inet-ref]                         | [**`InetAddress`**][java-inet-ref]|
| [`integer`][psql-integer-ref]                   | [**`Integer`**][java-integer-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Long`][java-long-ref]|
| [`interval`][psql-interval-ref]                 | Not yet supported.|
| [`json`][psql-json-ref]                         | Not yet supported.|
| [`line`][psql-line-ref]                         | Not yet supported.|
| [`lseg`][psql-lseq-ref]                         | Not yet supported.|
| [`macaddr`][psql-macaddr-ref]                   | Not yet supported.|
| [`macaddr8`][psql-macaddr8-ref]                 | Not yet supported.|
| [`money`][psql-money-ref]                       | Not yet supported.|
| [`numeric`][psql-bignumeric-ref]                | [`BigDecimal`][java-bigdecimal-ref]|
| [`path`][psql-path-ref]                         | Not yet supported.|
| [`pg_lsn`][psql-pg_lsn-ref]                     | Not yet supported.|
| [`point`][psql-point-ref]                       | Not yet supported.|
| [`polygon`][psql-polygon-ref]                   | Not yet supported.|
| [`real`][psql-real-ref]                         | [**`Float`**][java-float-ref], [`Double`][java-double-ref]|
| [`smallint`][psql-smallint-ref]                 | [**`Short`**][java-short-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Integer`][java-integer-ref], [`Long`][java-long-ref]|
| [`smallserial`][psql-smallserial-ref]           | [**`Integer`**][java-integer-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Long`][java-long-ref]|
| [`serial`][psql-serial-ref]                     | [**`Long`**][java-long-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref]|
| [`text`][psql-text-ref]                         | [`String`][java-string-ref]|
| [`time [without time zone]`][psql-time-ref]     | [`LocalTime`][java-lt-ref]|
| [`time [with time zone]`][psql-time-ref]        | Not yet supported.|
| [`timestamp [without time zone]`][psql-time-ref]|[`LocalDateTime`][java-ldt-ref]|
| [`timestamp [with time zone]`][psql-time-ref]   |[`ZonedDateTime`][java-zdt-ref]|
| [`tsquery`][psql-tsquery-ref]                   | Not yet supported.|
| [`tsvector`][psql-tsvector-ref]                 | Not yet supported.|
| [`txid_snapshot`][psql-txid_snapshot-ref]       | Not yet supported.|
| [`uuid`][psql-uuid-ref]                         | [**`UUID`**][java-uuid-ref], [`String`][java-string-ref]||
| [`xml`][psql-xml-ref]                           | Not yet supported. |

Types in **bold** indicate the native (default) Java type.

Support for the following single-dimensional arrays (read and write):

| PostgreSQL Type                                | Supported Data Type                  |
|:-----------------------------------------------|:-------------------------------------|
|[`text[]`][psql-text-ref]                        |[`String[]`][java-string-ref]         |  
|[`integer[] or int[]`][psql-integer-ref]        |[`Integer[]`][java-integer-ref], [`Long[]`][java-long-ref], [`Short[]`][java-short-ref]|


[psql-bigint-ref]: https://www.postgresql.org/docs/11/datatype-numeric.html#DATATYPE-INT
[psql-bit-ref]: https://www.postgresql.org/docs/11/datatype-numeric.html
[psql-boolean-ref]: https://www.postgresql.org/docs/11/datatype-boolean.html
[psql-box-ref]: https://www.postgresql.org/docs/11/datatype-geometric.html#id-1.5.7.16.8
[psql-bytea-ref]: https://www.postgresql.org/docs/11/datatype-binary.html#id-1.5.7.12.9
[psql-character-ref]: https://www.postgresql.org/docs/11/datatype-character.html
[psql-cidr-ref]: https://www.postgresql.org/docs/11/datatype-net-types.html#DATATYPE-CIDR
[psql-circle-ref]: https://www.postgresql.org/docs/11/datatype-geometric.html#DATATYPE-CIRCLE
[psql-date-ref]: https://www.postgresql.org/docs/11/datatype-datetime.html
[psql-floating-point-ref]: https://www.postgresql.org/docs/11/datatype-numeric.html#DATATYPE-FLOAT
[psql-inet-ref]: https://www.postgresql.org/docs/11/datatype-net-types.html#DATATYPE-INET
[psql-integer-ref]: https://www.postgresql.org/docs/11/datatype-numeric.html#DATATYPE-INT
[psql-interval-ref]: https://www.postgresql.org/docs/11/datatype-datetime.html#DATATYPE-INTERVAL-INPUT
[psql-json-ref]: https://www.postgresql.org/docs/11/datatype-json.html
[psql-line-ref]: https://www.postgresql.org/docs/11/datatype-geometric.html#DATATYPE-LINE
[psql-lseq-ref]: https://www.postgresql.org/docs/11/datatype-geometric.html#DATATYPE-LINE
[psql-bignumeric-ref]: https://www.postgresql.org/docs/11/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
[psql-macaddr-ref]: https://www.postgresql.org/docs/11/datatype-net-types.html#DATATYPE-MACADDR
[psql-macaddr8-ref]: https://www.postgresql.org/docs/11/datatype-net-types.html#DATATYPE-MACADDR8
[psql-money-ref]: https://www.postgresql.org/docs/11/datatype.html
[psql-path-ref]: https://www.postgresql.org/docs/11/datatype-geometric.html#id-1.5.7.16.9
[psql-pg_lsn-ref]: https://www.postgresql.org/docs/11/datatype-pg-lsn.html
[psql-point-ref]: https://www.postgresql.org/docs/11/datatype-geometric.html#id-1.5.7.16.5
[psql-polygon-ref]: https://www.postgresql.org/docs/11/datatype-geometric.html#DATATYPE-POLYGON
[psql-real-ref]: https://www.postgresql.org/docs/11/datatype.html
[psql-smallint-ref]: https://www.postgresql.org/docs/11/datatype.html
[psql-smallserial-ref]: https://www.postgresql.org/docs/11/datatype-numeric.html#DATATYPE-SERIAL
[psql-serial-ref]: https://www.postgresql.org/docs/11/datatype-numeric.html#DATATYPE-SERIAL
[psql-text-ref]: https://www.postgresql.org/docs/11/datatype.html
[psql-time-ref]: https://www.postgresql.org/docs/11/datatype-datetime.html
[psql-tsquery-ref]: https://www.postgresql.org/docs/11/datatype-textsearch.html#DATATYPE-TSQUERY
[psql-tsvector-ref]: https://www.postgresql.org/docs/11/datatype-textsearch.html#DATATYPE-TSVECTOR
[psql-txid_snapshot-ref]: https://www.postgresql.org/docs/11/datatype.html
[psql-uuid-ref]: https://www.postgresql.org/docs/11/datatype-uuid.html
[psql-xml-ref]: https://www.postgresql.org/docs/11/datatype-xml.html


[java-bigdecimal-ref]: https://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html
[java-boolean-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Boolean.html
[java-byte-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Byte.html
[java-double-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Double.html
[java-float-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Float.html
[java-inet-ref]: https://docs.oracle.com/javase/7/docs/api/java/net/InetAddress.html
[java-instant-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/Instant.html
[java-integer-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Integer.html
[java-long-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Long.html
[java-ldt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html
[java-ld-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDate.html
[java-lt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalTime.html
[java-odt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/OffsetDateTime.html
[java-short-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Short.html
[java-string-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/String.html
[java-uuid-ref]: https://docs.oracle.com/javase/8/docs/api/java/util/UUID.html
[java-zdt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html

## License
This project is released under version 2.0 of the [Apache License][l].

[l]: https://www.apache.org/licenses/LICENSE-2.0
