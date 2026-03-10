use crate::{
    error::LogQueryError,
    indices::Indices,
    ingest::find_logs_by_offsets,
    metadata::Metadata,
    parser::{Expr, Operator, ValueType, parse_query},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::RwLock;

pub async fn run_query(
    input: &str,
    indices: &Arc<RwLock<Indices>>,
    metadata: Arc<RwLock<Metadata>>,
) -> Result<(), LogQueryError> {
    match parse_query(input) {
        Ok(Some(ast)) => {
            let indices_read = indices.read().await;
            let offsets = evaluate(&ast, &indices_read)?;
            let logs = find_logs_by_offsets(&offsets, metadata).await?;

            if logs.is_empty() && !matches!(ast, Expr::Explain(_)) {
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

fn get_map<'a>(
    selector: &'a str,
    indices: &'a Indices,
) -> Result<&'a BTreeMap<String, BTreeSet<u64>>, LogQueryError> {
    match selector {
        "level" => Ok(&indices.levels),
        "word" => Ok(&indices.words),
        _ => Err(LogQueryError::UnknownSelector(selector.to_string())),
    }
}

fn get_rev_map<'a>(
    selector: &'a str,
    indices: &'a Indices,
) -> Result<&'a BTreeMap<String, BTreeSet<u64>>, LogQueryError> {
    match selector {
        "word" => Ok(&indices.rev_words),
        _ => Err(LogQueryError::UnknownSelector(selector.to_string())),
    }
}

pub fn get_prefix_matches(
    v: &str,
    map: &BTreeMap<String, BTreeSet<u64>>,
) -> Result<BTreeSet<u64>, LogQueryError> {
    let mut result = BTreeSet::new();

    let start = v.to_string();
    let end = format!("{}{{", v);

    for (_, set) in map.range(start..end) {
        result.extend(set.iter().cloned());
    }

    Ok(result)
}

pub fn get_suffix_matches(
    v: &str,
    rev_map: &BTreeMap<String, BTreeSet<u64>>,
) -> Result<BTreeSet<u64>, LogQueryError> {
    let mut result = BTreeSet::new();
    let reversed_query: String = v.chars().rev().collect();
    let start = reversed_query.clone();
    let end = format!("{}{{", reversed_query);

    for (_, set) in rev_map.range(start..end) {
        result.extend(set.iter().cloned());
    }

    Ok(result)
}

pub fn evaluate(expr: &Expr, indices: &Indices) -> Result<BTreeSet<u64>, LogQueryError> {
    match expr {
        Expr::Explain(inner) => {
            println!("{}", inner);
            Ok(BTreeSet::new())
        }

        Expr::Condition { selector, value } => match value {
            ValueType::Full(v) => Ok(get_map(selector, indices)?
                .get(v)
                .cloned()
                .unwrap_or_default()),

            ValueType::StartsWith(v) => get_prefix_matches(v, get_map(selector, indices)?),

            ValueType::EndsWith(v) => get_suffix_matches(v, get_rev_map(selector, indices)?),

            ValueType::Contains(v) => {
                let map = get_map(selector, indices)?;
                let mut result = BTreeSet::new();
                println!("{}", v);

                for (word, offsets) in map {
                    if word.contains(v) {
                        result.extend(offsets);
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
