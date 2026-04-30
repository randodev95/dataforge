use DataForge::{Engine, types::EnvName};
use std::time::Instant;

fn main() -> anyhow::Result<()> {
    let mut engine = Engine::new();
    let model_count = 10_000;
    
    let dev = EnvName("dev".to_string());
    let prod = EnvName("prod".to_string());

    println!("--- DataForge Scalability Benchmark ---");
    println!("Target: {} models", model_count);
    
    // 1. Generation
    let start_gen = Instant::now();
    let mut models = Vec::new();
    for i in 0..model_count {
        let content = if i == 0 {
            format!("model(name='model_{}', query='SELECT 1')\n", i)
        } else {
            format!("model(name='model_{}', query='SELECT * FROM ' + ref('model_{}'))\n", i, i-1)
        };
        models.push(content);
    }
    println!("Generated models in: {:?}", start_gen.elapsed());
    
    // 2. Registration (Manifest Gen)
    let start_reg = Instant::now();
    for content in models.iter() {
        engine.register_model(&dev, content)?;
    }
    let reg_time = start_reg.elapsed();
    println!("Registered {} models in: {:?}", model_count, reg_time);
    println!("Average registration time per model: {:?}", reg_time / model_count as u32);
    
    // 3. Planning
    let start_plan = Instant::now();
    let _plan = engine.plan(&dev, &prod)?;
    println!("Generated plan in: {:?}", start_plan.elapsed());
    
    Ok(())
}
