use titan_engine::cli::{Cli, Commands, ExposureAction, handle_init, handle_pipeline, handle_test, handle_exposure};
use clap::Parser;
use anyhow::Result;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // JSON logging for CI compatibility
    tracing_subscriber::fmt()
        .json()
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init { name, path } => {
            handle_init(name, path).await?;
        }
        Commands::Plan { target, select, path } => {
            handle_pipeline(path, target, select, true).await?;
        }
        Commands::Run { target, select, path } => {
            handle_pipeline(path, target, select, false).await?;
        }
        Commands::Test { target, path } => {
            handle_test(path, target).await?;
        }
        Commands::Exposure { action } => {
            match action {
                ExposureAction::List { path } => {
                    handle_exposure(path).await?;
                }
            }
        }
    }

    Ok(())
}
