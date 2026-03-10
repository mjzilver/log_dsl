use std::fmt::Display;

use crate::error::LogQueryError;

#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    And,
    Or,
    Not,
}

impl Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operator::And => write!(f, "AND"),
            Operator::Or => write!(f, "OR"),
            Operator::Not => write!(f, "NOT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueType {
    Full(String),
    StartsWith(String),
    EndsWith(String),
    Contains(String),
}

impl Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Full(v) => write!(f, "Full match: [{}]", v),
            ValueType::StartsWith(v) => write!(f, "StartsWith: [{}]", v),
            ValueType::EndsWith(v) => write!(f, "EndsWith: [{}]", v),
            ValueType::Contains(v) => write!(f, "Contains: [{}]", v),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Condition {
        selector: String,
        value: ValueType,
    },
    Explain(Box<Expr>),
    Unary {
        op: Operator,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
}

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Condition { selector, value } => {
                write!(f, "Selector [{}], Value [{}]", selector, value)
            }
            Expr::Unary { op, expr } => {
                write!(f, "Op [{}], Expr [{}]", op, expr)
            }
            Expr::Explain(inner) => {
                write!(f, "EXPLAIN [{}]", inner)
            }
            Expr::Binary { left, op, right } => {
                write!(f, "Left [{}], Op [{}], Right [{}]", left, op, right)
            }
        }
    }
}

pub fn parse_query(input: &str) -> Result<Option<Expr>, LogQueryError> {
    let tokens = tokenize(input);
    let mut explain = false;
    let mut start = 0usize;

    if let Some(t) = tokens.first()
        && *t == Token::Explain
    {
        explain = true;
        start = 1;
    }

    let mut iter = tokens.into_iter().skip(start);

    let mut left = match parse_condition(&mut iter)? {
        Some(expr) => expr,
        None => return Ok(None),
    };

    while let Some(token) = iter.next() {
        match token {
            Token::And => {
                let right = parse_condition(&mut iter)?.ok_or_else(|| {
                    LogQueryError::ParserError("Expected expression after AND".into())
                })?;

                left = Expr::Binary {
                    left: Box::new(left),
                    op: Operator::And,
                    right: Box::new(right),
                };
            }

            Token::Or => {
                let right = parse_condition(&mut iter)?.ok_or_else(|| {
                    LogQueryError::ParserError("Expected expression after OR".into())
                })?;

                left = Expr::Binary {
                    left: Box::new(left),
                    op: Operator::Or,
                    right: Box::new(right),
                };
            }

            Token::Not => {
                let expr = parse_condition(&mut iter)?.ok_or_else(|| {
                    LogQueryError::ParserError("Expected expression after NOT".into())
                })?;

                left = Expr::Unary {
                    op: Operator::Not,
                    expr: Box::new(expr),
                };
            }

            _ => {
                return Err(LogQueryError::ParserError("Unexpected token".into()));
            }
        }
    }

    if explain {
        Ok(Some(Expr::Explain(Box::new(left))))
    } else {
        Ok(Some(left))
    }
}

fn parse_value(
    iter: &mut impl Iterator<Item = Token>,
    selector: String,
    constructor: fn(String) -> ValueType,
    err: &'static str,
) -> Result<Option<Expr>, LogQueryError> {
    match iter.next() {
        Some(Token::Ident(value)) => Ok(Some(Expr::Condition {
            selector,
            value: constructor(value),
        })),
        _ => Err(LogQueryError::ParserError(err.into())),
    }
}

fn parse_condition(iter: &mut impl Iterator<Item = Token>) -> Result<Option<Expr>, LogQueryError> {
    match iter.next() {
        Some(Token::Ident(selector)) => {
            match iter.next() {
                Some(Token::Equals) => {}
                _ => {
                    return Err(LogQueryError::ParserError(
                        "Expected '=' after selector".into(),
                    ));
                }
            }

            match iter.next() {
                Some(Token::StartsWith) => parse_value(
                    iter,
                    selector,
                    ValueType::StartsWith,
                    "Expected value after '^'",
                ),

                Some(Token::EndsWith) => parse_value(
                    iter,
                    selector,
                    ValueType::EndsWith,
                    "Expected value after '$'",
                ),

                Some(Token::Contains) => parse_value(
                    iter,
                    selector,
                    ValueType::Contains,
                    "Expected value after '~'",
                ),

                Some(Token::Ident(value)) => Ok(Some(Expr::Condition {
                    selector,
                    value: ValueType::Full(value),
                })),

                _ => Err(LogQueryError::ParserError(
                    "Expected value after '='".into(),
                )),
            }
        }

        Some(Token::Not) => {
            let expr = parse_condition(iter)?.ok_or_else(|| {
                LogQueryError::ParserError("Expected expression after NOT".into())
            })?;

            Ok(Some(Expr::Unary {
                op: Operator::Not,
                expr: Box::new(expr),
            }))
        }

        Some(_) => Err(LogQueryError::ParserError(
            "Invalid start of expression".into(),
        )),

        None => Ok(None),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident(String), // Selector or Pattern
    Equals,
    And,
    Or,
    Not,
    Explain,
    LParen,
    RParen,
    // ^
    StartsWith,
    // $
    EndsWith,
    // ~
    Contains,
}

pub fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens: Vec<Token> = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            '(' => {
                tokens.push(Token::LParen);
                chars.next();
            }
            ')' => {
                tokens.push(Token::RParen);
                chars.next();
            }
            ' ' => {
                chars.next();
            }
            '=' => {
                tokens.push(Token::Equals);
                chars.next();
            }
            '^' => {
                tokens.push(Token::StartsWith);
                chars.next();
            }
            '$' => {
                tokens.push(Token::EndsWith);
                chars.next();
            }
            '~' => {
                tokens.push(Token::Contains);
                chars.next();
            }
            _ => {
                let mut word = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_alphabetic() {
                        word.push(ch);
                        chars.next();
                    } else {
                        break;
                    }
                }

                tokens.push(tokenize_word(word));
            }
        }
    }

    tokens
}

pub fn tokenize_word(str: String) -> Token {
    match str.to_uppercase().as_str() {
        "AND" => Token::And,
        "OR" => Token::Or,
        "NOT" => Token::Not,
        "EXPLAIN" => Token::Explain,
        _ => Token::Ident(str),
    }
}
