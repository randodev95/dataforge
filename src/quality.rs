use anyhow::Result;
use crate::execution::Muscle;
use async_trait::async_trait;

#[async_trait]
pub trait Test {
    fn name(&self) -> &str;
    async fn run(&self, muscle: &Muscle) -> Result<()>;
}

pub struct UniqueTest {
    pub model: String,
    pub column: String,
}

#[async_trait]
impl Test for UniqueTest {
    fn name(&self) -> &str { "unique" }
    async fn run(&self, muscle: &Muscle) -> Result<()> {
        let sql = format!("SELECT {} FROM {} GROUP BY {} HAVING COUNT(*) > 1", self.column, self.model, self.column);
        let batches = muscle.execute_and_fetch(&sql).await?;
        let count: usize = batches.iter().map(|b| b.num_rows()).sum();
        if count > 0 {
            Err(anyhow::anyhow!("Test failed: {} is not unique in {}", self.column, self.model))
        } else {
            Ok(())
        }
    }
}

pub struct NotNullTest {
    pub model: String,
    pub column: String,
}

#[async_trait]
impl Test for NotNullTest {
    fn name(&self) -> &str { "not_null" }
    async fn run(&self, muscle: &Muscle) -> Result<()> {
        let sql = format!("SELECT * FROM {} WHERE {} IS NULL", self.model, self.column);
        let batches = muscle.execute_and_fetch(&sql).await?;
        let count: usize = batches.iter().map(|b| b.num_rows()).sum();
        if count > 0 {
            Err(anyhow::anyhow!("Test failed: {} contains NULL values in {}", self.column, self.model))
        } else {
            Ok(())
        }
    }
}
