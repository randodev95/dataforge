use starlark::environment::GlobalsBuilder;
use starlark::eval::Evaluator;
use starlark::starlark_module;
use starlark::values::Value;
use starlark::any::ProvidesStaticType;
use std::cell::RefCell;

#[derive(ProvidesStaticType, Clone)]
pub struct StarlarkContext {
    pub refs: RefCell<Vec<crate::types::ModelName>>,
    pub engine: crate::Engine,
    pub env: crate::types::EnvName,
    pub sql_body: Option<String>,
}

#[starlark_module]
pub fn dataforge_globals(builder: &mut GlobalsBuilder) {
    fn model<'v>(
        #[starlark(default = "")] name: &str,
        #[starlark(default = "")] query: &str,
        #[starlark(default = starlark::values::list_or_tuple::UnpackListOrTuple::default())] columns: starlark::values::list_or_tuple::UnpackListOrTuple<String>,
        watermark: Option<String>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let ctx = eval.extra.and_then(|e| e.downcast_ref::<StarlarkContext>()).unwrap();
        let deps = ctx.refs.borrow().clone();
        
        let final_query = if query.is_empty() {
            ctx.sql_body.clone().unwrap_or_default()
        } else {
            query.to_string()
        };

        let inferred_cols = ctx.engine.extract_columns(&final_query, ctx.engine.dialect()).unwrap_or_default();
        
        ctx.engine.internal_add_model(&ctx.env, crate::types::Model {
            name: crate::types::ModelName(name.to_string()),
            query: final_query,
            deps,
            contracts: columns.items,
            watermark,
            inferred_columns: inferred_cols,
        });
        Ok(Value::new_none())
    }

    fn r#ref<'v>(name: &str, eval: &mut Evaluator<'v, '_, '_>) -> anyhow::Result<String> {
        let ctx = eval.extra.and_then(|e| e.downcast_ref::<StarlarkContext>()).unwrap();
        ctx.refs.borrow_mut().push(crate::types::ModelName(name.to_string()));
        Ok(name.to_string())
    }
}
