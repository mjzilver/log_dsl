use crate::{
    error::LogQueryError,
    indices::Indices,
    ingest::find_logs_by_offsets,
    parser::{Expr, Operator, ValueType, parse_query},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::RwLock;

pub async fn run_query(input: &str, indices: &Arc<RwLock<Indices>>) -> Result<(), LogQueryError> {
    match parse_query(input) {
        Ok(Some(ast)) => {
            let indices_read = indices.read().await;
            let offsets = evaluate(&ast, &indices_read)?;
            let logs = find_logs_by_offsets(&offsets).await?;

            if logs.is_empty() {
                println!("No logs found");
            } else {
                for log in logs {
                    println!("{}", log);
                }
            }

            Ok(())
        }

        Ok(None) => {
            println!("No query provided");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn get_map<'a>(selector: &'a str, indices: &'a Indices) -> &'a BTreeMap<String, BTreeSet<u64>> {
    match selector {
        "level" => &indices.levels,
        "word" => &indices.words,
        _ => &indices.levels,
    }
}

pub fn evaluate(expr: &Expr, indices: &Indices) -> Result<BTreeSet<u64>, LogQueryError> {
    match expr {
        Expr::Explain(inner) => {
            println!("{}", inner);
            Ok(BTreeSet::new())
        }

        Expr::Condition { selector, value } => match value {
            ValueType::Full(v) => {
                let map = get_map(selector, indices);
                Ok(map.get(v).cloned().unwrap_or_default())
            }

            ValueType::StartsWith(str) => {
                let mut result = BTreeSet::new();
                let map = get_map(selector, indices);

                for (key, set) in map.iter() {
                    if key.starts_with(str) {
                        result.extend(set.clone());
                    }
                }

                Ok(result)
            }
        },

        Expr::Unary { op, expr: inner } => {
            let result = evaluate(inner, indices)?;

            match op {
                Operator::Not => Ok(&build_universe(indices) - &result),
                _ => Ok(result),
            }
        }

        Expr::Binary { left, op, right } => {
            let left_set = evaluate(left, indices)?;
            let right_set = evaluate(right, indices)?;

            match op {
                Operator::And => Ok(&left_set & &right_set),
                Operator::Or => Ok(&left_set | &right_set),
                _ => Ok(left_set),
            }
        }
    }
}

fn build_universe(indices: &Indices) -> BTreeSet<u64> {
    let mut universe = BTreeSet::new();

    for offsets in indices.levels.values() {
        universe.extend(offsets);
    }

    universe
}
