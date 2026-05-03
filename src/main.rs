use titan_engine::cli::{Cli, Commands, handle_init, handle_pipeline};
use clap::Parser;
use anyhow::Result;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize industry-standard tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init { name, path } => {
            handle_init(name, path).await?;
        }
        Commands::Plan { env, select, path } => {
            handle_pipeline(path, env, select, true).await?;
        }
        Commands::Run { env, select, path } => {
            handle_pipeline(path, env, select, false).await?;
        }
    }

    Ok(())
}
