use clap::{Parser, Subcommand};
use DataForge::project::Project;
use DataForge::{Engine, types::EnvName};
use std::path::PathBuf;

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
    /// Plan transformation for an environment
    Plan {
        #[arg(short, long)]
        project: PathBuf,
        #[arg(short, long, default_value = "dev")]
        from: String,
        #[arg(short, long, default_value = "prod")]
        to: String,
        #[arg(short, long, default_value = "generic")]
        dialect: String,
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
        Commands::Plan { project, from, to, dialect } => {
            let target_dialect: DataForge::TargetDialect = dialect.parse().unwrap();
            let mut engine = Engine::with_dialect(target_dialect);
            let proj = Project::load(&project)?;
            
            let from_env = EnvName(from);
            let to_env = EnvName(to);
            
            engine.load_project(&proj, &from_env)?;
            
            let plan = engine.plan(&from_env, &to_env)?;
            println!("Execution Plan:");
            for action in &plan.actions {
                println!("  - {:?}", action);
            }
        }
    }

    Ok(())
}
