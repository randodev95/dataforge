use clap::{Parser, Subcommand};
use DataForge::project::Project;
use DataForge::{Engine, types::EnvName};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "dataforge")]
#[command(about = "DataForge CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new DataForge project
    Init {
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Query a table or view
    Query {
        #[arg(short, long, default_value = "jaffle.db")]
        db: String,
        sql: String,
    },
    /// Start the engine in watch mode with API server
    Serve {
        #[arg(short, long)]
        project: PathBuf,
        #[arg(short, long, default_value = "8080")]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { path } => {
            Project::init(&path)?;
            println!("Initialized DataForge project at {:?}", path);
        }
        Commands::Query { db, sql } => {
            let conn = duckdb::Connection::open(db).unwrap();
            let mut stmt = conn.prepare(&sql).unwrap();
            let mut rows = stmt.query([]).unwrap();
            
            println!("Query results:");
            while let Some(row) = rows.next().unwrap() {
                // Heuristic: just print first 5 columns as strings
                let mut vals = vec![];
                for i in 0..5 {
                    if let Ok(val) = row.get::<usize, String>(i) {
                        vals.push(val);
                    }
                }
                println!("  {}", vals.join(" | "));
            }
        }
        Commands::Serve { project, port } => {
            let orchestrator = DataForge::orchestrator::Orchestrator::new(
                DataForge::orchestrator::OrchestratorConfig {
                    watch_mode: true,
                    preview_enabled: true,
                },
                project.clone()
            );
            
            let addr = format!("127.0.0.1:{}", port).parse().unwrap();
            let handler = DataForge::api::create_rpc_handler();
            let server = jsonrpc_http_server::ServerBuilder::new(handler)
                .start_http(&addr)
                .expect("Unable to start RPC server");
            
            println!("DataForge 2.0 Engine serving at {}", addr);
            println!("Orchestrator initialized (Watch Mode: ON)");
            
            let orchestrator_clone = Arc::new(orchestrator);
            let project_clone = project.clone();
            std::thread::spawn(move || {
                if let Err(e) = orchestrator_clone.watch(&project_clone) {
                    eprintln!("Orchestrator Watch Error: {}", e);
                }
            });

            server.wait();
        }
    }

    Ok(())
}
