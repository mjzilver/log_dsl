use crate::{
    error::LogQueryError,
    indices::Indices,
    parser::{Expr, Operator, parse_query},
};
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::RwLock;

pub async fn run_query(input: &str, indices: &Arc<RwLock<Indices>>) -> Result<(), LogQueryError> {
    match parse_query(input) {
        Ok(Some(ast)) => {
            let indices_read = indices.read().await;
            let offsets = evaluate(&ast, &indices_read)?;
            let logs = crate::ingest::find_logs_by_offsets(&offsets).await?;

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

pub fn evaluate(expr: &Expr, indices: &Indices) -> Result<BTreeSet<u64>, LogQueryError> {
    match expr {
        Expr::Explain(inner) => {
            println!("{}", inner);
            Ok(BTreeSet::new())
        }

        Expr::Condition { selector, value } => match selector.as_str() {
            "level" => Ok(indices.levels.get(value).cloned().unwrap_or_default()),
            "word" => Ok(indices.words.get(value).cloned().unwrap_or_default()),
            _ => Ok(BTreeSet::new()),
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
