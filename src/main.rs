use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use gcp_bigquery_client::model::query_request::QueryRequest;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::PgWireHandlerFactory;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;
use std::env;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::net::TcpListener;

pub struct BigQueryProcessor {
    client: Arc<gcp_bigquery_client::Client>,
    project_id: String,
}

#[async_trait]
impl SimpleQueryHandler for BigQueryProcessor {
    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if query.starts_with("SELECT") || query.starts_with("select") {
            let mut rs = self
                .client
                .job()
                .query(&self.project_id, QueryRequest::new(query))
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            // Fetch the column names and schema
            let mut columns = rs
                .column_names()
                .into_iter()
                .map(|name| (*rs.column_index(&name).unwrap(), name))
                .collect::<Vec<(usize, String)>>();

            columns.sort_by(|a, b| a.0.cmp(&b.0));

            let n_columns = columns.len();

            let schema = Arc::new(
                columns
                    .into_iter()
                    .map(|(_, name)| {
                        FieldInfo::new(name, None, None, Type::VARCHAR, FieldFormat::Text)
                    })
                    .collect::<Vec<_>>(),
            );

            let mut results = Vec::with_capacity(rs.row_count());
            while rs.next_row() {
                let mut encoder = DataRowEncoder::new(schema.clone());
                for index in 0..n_columns {
                    let v = rs
                        .get_string(index)
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    encoder.encode_field(&v)?;
                }
                results.push(encoder.finish());
            }

            let data_row_stream = stream::iter(results);

            Ok(vec![Response::Query(QueryResponse::new(
                schema,
                data_row_stream,
            ))])
        } else {
            client
                .send(PgWireBackendMessage::ErrorResponse(ErrorResponse::from(
                    ErrorInfo::new(
                        "ERROR".to_owned(),
                        "0A000".to_owned(),
                        format!("Unsupported query: {}", query),
                    ),
                )))
                .await?;
            Ok(vec![])
        }
    }
}

struct BigQueryProcessorFactory {
    handler: Arc<BigQueryProcessor>,
}

impl PgWireHandlerFactory for BigQueryProcessorFactory {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = BigQueryProcessor;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(NoopStartupHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    // Read configuration parameters from environment variables
    let project_id = env::var("PROJECT_ID").expect("Environment variable PROJECT_ID");
    let gcp_sa_key = env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .expect("Environment variable GOOGLE_APPLICATION_CREDENTIALS");

    // Init BigQuery client
    let client = gcp_bigquery_client::Client::from_service_account_key_file(&gcp_sa_key).await?;

    let factory = Arc::new(BigQueryProcessorFactory {
        handler: Arc::new(BigQueryProcessor {
            client: Arc::new(client),
            project_id: project_id.clone(),
        }),
    });

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();

        tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}
