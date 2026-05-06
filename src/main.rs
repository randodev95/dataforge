use anyhow::Result;
use clap::Parser;
use titan_engine::cli::{
    Cli, Commands, ExposureAction, deps::handle_deps, freshness::handle_freshness, handle_check,
    handle_estimate, handle_exposure, handle_init, handle_lineage, handle_lineage_diff,
    handle_optimize, handle_pipeline, handle_test, handle_unit_test, promote::handle_promote,
    status::handle_status,
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let project_path = match &cli.command {
        Commands::Exposure { action } => match action {
            ExposureAction::List { path } => path,
        },
        Commands::Init { path, .. }
        | Commands::Plan { path, .. }
        | Commands::Run { path, .. }
        | Commands::Test { path, .. }
        | Commands::UnitTest { path, .. }
        | Commands::Lineage { path, .. }
        | Commands::LineageDiff { path, .. }
        | Commands::Optimize { path, .. }
        | Commands::Setup { path, .. }
        | Commands::Check { path, .. }
        | Commands::Estimate { path, .. }
        | Commands::Compare { path, .. }
        | Commands::Promote { path, .. }
        | Commands::Status { path, .. }
        | Commands::Deps { path, .. }
        | Commands::Freshness { path, .. } => path,
    };

    if let Err(e) = titan_engine::telemetry::init_telemetry(project_path) {
        eprintln!("Failed to init telemetry: {e}");
    }

    if cli.metrics {
        titan_engine::metrics::register_metrics();
        tokio::spawn(async move {
            use hyper::{
                Body, Request, Response, Server,
                service::{make_service_fn, service_fn},
            };
            use prometheus::{Encoder, TextEncoder};

            let addr = ([127, 0, 0, 1], 9090).into();

            let make_svc = make_service_fn(|_conn| async {
                Ok::<_, hyper::Error>(service_fn(|_req: Request<Body>| async {
                    let mut buffer = Vec::new();
                    let encoder = TextEncoder::new();
                    let metric_families = prometheus::gather();
                    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
                        tracing::error!("Failed to encode metrics: {}", e);
                    }

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
            handle_init(name, path)?;
        }
        Commands::Plan {
            target,
            select,
            state,
            path,
            jobs,
            shadow,
            allow_drift,
            grace_period,
        } => {
            handle_pipeline(
                path,
                target,
                select,
                state,
                true,
                jobs,
                shadow,
                allow_drift,
                grace_period,
                None,
            )
            .await?;
        }
        Commands::Run {
            target,
            select,
            state,
            path,
            jobs,
            shadow,
            allow_drift,
            grace_period,
            quarantine,
        } => {
            handle_pipeline(
                path,
                target,
                select,
                state,
                false,
                jobs,
                shadow,
                allow_drift,
                grace_period,
                quarantine,
            )
            .await?;
        }
        Commands::Test { target, path } => {
            handle_test(path, target).await?;
        }
        Commands::UnitTest { path } => {
            handle_unit_test(path).await?;
        }
        Commands::Lineage { model, path } => {
            handle_lineage(path, model)?;
        }
        Commands::Optimize {
            measure_dedup,
            target,
            path,
        } => {
            handle_optimize(path, target, measure_dedup).await?;
        }
        Commands::LineageDiff {
            model_a,
            model_b,
            path,
        } => {
            handle_lineage_diff(path, model_a, model_b)?;
        }
        Commands::Check { target, path } => {
            handle_check(path, target).await?;
        }
        Commands::Estimate {
            target,
            warehouse,
            path,
        } => {
            handle_estimate(path, target, warehouse).await?;
        }
        Commands::Exposure { action } => match action {
            ExposureAction::List { path } => {
                handle_exposure(path)?;
            }
        },
        Commands::Compare {
            model,
            base,
            target,
            path,
        } => {
            titan_engine::cli::handle_compare(path, model, base, target).await?;
        }
        Commands::Setup { driver, path } => {
            titan_engine::cli::setup::handle_setup(path, driver)?;
        }
        Commands::Promote {
            model,
            from,
            target,
            path,
        } => {
            handle_promote(path, model, from, target).await?;
        }
        Commands::Status { target, path } => {
            handle_status(path, target)?;
        }
        Commands::Deps { path } => {
            handle_deps(path)?;
        }
        Commands::Freshness { path } => {
            handle_freshness(path).await?;
        }
    }

    Ok(())
}
