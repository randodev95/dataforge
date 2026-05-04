use titan_engine::cli::{Cli, Commands, ExposureAction, handle_init, handle_pipeline, handle_test, handle_exposure, handle_check};
use clap::Parser;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let project_path = match &cli.command {
        Commands::Init { path, .. } => path,
        Commands::Plan { path, .. } => path,
        Commands::Run { path, .. } => path,
        Commands::Test { path, .. } => path,
        Commands::Setup { path, .. } => path,
        Commands::Check { path, .. } => path,
        Commands::Exposure { action } => match action {
            ExposureAction::List { path } => path,
        },
    };

    if let Err(e) = titan_engine::telemetry::init_telemetry(project_path) {
        eprintln!("Failed to init telemetry: {}", e);
    }

    if cli.metrics {
        titan_engine::metrics::register_metrics();
        tokio::spawn(async move {
            use hyper::{
                service::{make_service_fn, service_fn},
                Body, Request, Response, Server,
            };
            use prometheus::{Encoder, TextEncoder};

            let addr = ([127, 0, 0, 1], 9090).into();

            let make_svc = make_service_fn(|_conn| async {
                Ok::<_, hyper::Error>(service_fn(|_req: Request<Body>| async {
                    let mut buffer = Vec::new();
                    let encoder = TextEncoder::new();
                    let metric_families = prometheus::gather();
                    encoder.encode(&metric_families, &mut buffer).unwrap();

                    Ok::<_, hyper::Error>(Response::new(Body::from(buffer)))
                }))
            });

            let server = Server::bind(&addr).serve(make_svc);
            tracing::info!("Metrics server listening on http://{}", addr);

            if let Err(e) = server.await {
                tracing::error!("Metrics server error: {}", e);
            }
        });
    }

    match cli.command {
        Commands::Init { name, path } => {
            handle_init(name, path).await?;
        }
        Commands::Plan { target, select, state, path, jobs } => {
            handle_pipeline(path, target, select, state, true, jobs).await?;
        }
        Commands::Run { target, select, state, path, jobs } => {
            handle_pipeline(path, target, select, state, false, jobs).await?;
        }
        Commands::Test { target, path } => {
            handle_test(path, target).await?;
        }
        Commands::Check { target, path } => {
            handle_check(path, target).await?;
        }
        Commands::Exposure { action } => {
            match action {
                ExposureAction::List { path } => {
                    handle_exposure(path).await?;
                }
            }
        }
        Commands::Setup { driver, path } => {
            titan_engine::cli::setup::handle_setup(path, driver).await?;
        }
    }

    Ok(())
}
