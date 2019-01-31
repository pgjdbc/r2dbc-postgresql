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
  <version>1.0.0.M5</version>
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

Mono<Connection> connection = connectionFactory.create();
```

Supported Connection Factory Discovery options:

| Option | Description
| ------ | -----------
| `driver` | Must be `postgresql`
| `host` | Server hostname to connect to
| `port` | Server port to connect to.  Defaults to 1443. _(Optional)_
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

Mono<Connection> connection = connectionFactory.create();
```

PostgreSQL uses index parameters that are prefixed with `$`.  The following SQL statement makes use of parameters:

```sql
INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3)
```
Parameters are referenced using the same identifiers when binding these:

```java
connection
    .createStatement("INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3)")
    .bind("$1", 1)
    .bind("$2", "Walter")
    .bind("$3", "White")
    .execute()
```

Binding also allowed positional index (zero-based) references.  The parameter index is derived from the parameter discovery order when parsing the query.

## License
This project is released under version 2.0 of the [Apache License][l].

[l]: https://www.apache.org/licenses/LICENSE-2.0
