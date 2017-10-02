# rowmetrics
This is a small tool to calculate the rate of insertion for certain database tables and publish them as cloud metrics.

# Table of Contents
- [Purpose](#purpose)
    + [Example](#example)
    + [Support](#support)
      - [Databases](#databases)
      - [Cloud Metrics](#cloud-metrics)
- [Setup](#setup)
    + [Configuration](#configuration)
- [Usage](#usage)
- [Limitations](#limitations)

# Purpose
In some applications, the amount of rows being inserted into a database table can offer crucial visibility to an Ops team to understand how the application is being used. This is a tool to be ran at a specified interval in order to publish the amount of tables inserted since the last run as a cloudwatch metric.

### Example
Say a company has a table that represents every message sent. This table is called `Message`. The `rowmetrics` tool could be installed on a machine with access to this database and set to run every five minutes. When the tool is ran, it will perform the following steps:
 * Look through the configuration and retrieve the tables to get row counts for
 * Retrieve the row counts for these tables
 * Check if a previous session's data is stored. If it is, load it. Otherwise, write the current values and exit
 * Retrieve the difference between the two sessions
 * Publish the difference as a set of cloud metrics

This allows for trends in row insertion to be graphed and even acted on using the cloud metric tool chosen

### Support
#### Databases
Currently, `rowmetrics` supports the following databases:
 * MySQL
 * PostgreSQL

#### Cloud Metrics
Currently, `rowmetrics` can push metrics to the following providers:
 * Amazon Web Services Cloudwatch

# Setup
### Configuration
A sample configuration file in included in this repository at `examples/config.example.yml`

A configuration is composed of the following values:

`countPath`: Path to the counts YAML file to be written/read from

`aws`: Amazon Web Services configuration data

`aws.region`: Region a set of credentials belongs to

`aws.accessKeyId`: Access Key ID of a set of credentials

`aws.secretAccessKey`: Secret Access Key of a set of credentials

`aws.namespace`: OPTIONAL: The namespace to publish metrics in. Defaults to "RowMetrics"

`databases`: A list of databases to publish rowmetrics for

`database.name`: Name of the database, to be used as an identifier in the counts YAML as well as the identifier in the published metric dimension

`database.host`: Host of the database, fully specified `HOST:PORT` for the the tool to connect to

`database.type`: Type of database, set to a specified supported database. Defaults to "mysql"

`database.schema`: Schema of the datbase. Defaults to the database name in MySQL or "public" in PostgreSQL

`database.user`: User the tool will use to connect to the database

`database.password`: Plaintext password the tool will use to connect to the database

`database.tables`: Two lists representing sets of tables to have data retrieved for

`database.tables.increment`: List of tables to have their auto increment values retrieved for

`database.tables.row`: List of tables to have their (approximate) row count retrieved for

# Usage
To run `rowmetrics`, simply invoke the command, and it will do the rest:

```
./rowmetrics
```

This will attempt to load `config.yml` in the current working directory. To explicitly specify the path to a config YAML file, use the config flag:

```
./rowmetrics -config=/path/to/config.yml
```

# Limitations
 * In PostgreSQL, it is non-trivial to obtain the auto-increment value for a table itself. Therefore, both `increment` and `row` will retrieve row count if the database is PostgreSQL. The program will WARN as such.
 * The tool is currently not "stateless" and requires a place to write a counts file from the previous session. There are several options to be explored for providing a less machine-dependent method of previous session storage.