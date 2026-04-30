use jsonrpc_core::{Result as RpcResult, IoHandler};
use jsonrpc_derive::rpc;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct SuggestionResponse {
    pub columns: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct LineageResponse {
    pub edges: Vec<(String, String)>,
}

pub trait DataForgeRpc {
    fn get_column_suggestions(&self, model: String) -> RpcResult<SuggestionResponse>;
    fn get_lineage_graph(&self) -> RpcResult<LineageResponse>;
    fn trigger_plan(&self, env: String) -> RpcResult<String>;
}

pub struct DataForgeApi;

impl DataForgeRpc for DataForgeApi {
    fn get_column_suggestions(&self, _model: String) -> RpcResult<SuggestionResponse> {
        Ok(SuggestionResponse {
            columns: vec!["id".into(), "created_at".into(), "status".into()],
        })
    }

    fn get_lineage_graph(&self) -> RpcResult<LineageResponse> {
        Ok(LineageResponse {
            edges: vec![("stg_orders".into(), "fct_orders".into())],
        })
    }

    fn trigger_plan(&self, env: String) -> RpcResult<String> {
        Ok(format!("Plan triggered for environment: {}", env))
    }
}

pub fn create_rpc_handler() -> IoHandler {
    let _io = IoHandler::new();
    // TODO: Map methods manually to avoid RpcMethodSimple issues
    IoHandler::new()
}
