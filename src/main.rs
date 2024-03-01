use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use gcp_bigquery_client::model::query_request::QueryRequest;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
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

            let column_names = rs.column_names();

            let schema = Arc::new(
                column_names
                    .iter()
                    .map(|c| {
                        FieldInfo::new(c.clone(), None, None, Type::VARCHAR, FieldFormat::Text)
                    })
                    .collect::<Vec<_>>(),
            );

            let mut results = Vec::with_capacity(rs.row_count());
            while rs.next_row() {
                let mut encoder = DataRowEncoder::new(schema.clone());
                for name in column_names.iter() {
                    let v = rs
                        .get_string_by_name(name)
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

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    // Read configuration parameters from environment variables
    let project_id = env::var("PROJECT_ID").expect("Environment variable PROJECT_ID");
    let gcp_sa_key = env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .expect("Environment variable GOOGLE_APPLICATION_CREDENTIALS");

    // Init BigQuery client
    let client = gcp_bigquery_client::Client::from_service_account_key_file(&gcp_sa_key).await?;

    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(BigQueryProcessor {
        client: Arc::new(client),
        project_id: project_id.clone(),
    })));
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await
        });
    }
}
