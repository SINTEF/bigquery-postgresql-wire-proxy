# BigQuery PostgreSQL Wire Proxy

A proof of concept PostgreSQL wire protocol proxy server for BigQuery.

In case you want to query Google BigQuery, but using a PostgreSQL client. The PostgreSQL wire protocol is quite permissive so you can write BigQuery SQL and get the results back.

Only `SELECT` statements are supported.

Everything is done in memory, so it's not suitable for large result sets. And everything is serialized to strings.

This is very much a proof of concept and not suitable for production use.

As the [LICENSE](LICENSE) says, this is provided as-is with no warranty or support.

Thanks for the [gcp-bigquery-client](https://crates.io/crates/gcp-bigquery-client) and [pgwire](https://crates.io/crates/pgwire) crates for making this possible.