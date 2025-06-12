// Copyright (c) 2024 InfluxData Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversionError {
    UnsupportedExpression(String),
    UnsupportedLiteral(String),
    UnsupportedOperator(String),
    InternalError(String),
}

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConversionError::UnsupportedExpression(expr_type) => {
                write!(f, "Unsupported expression type: {}", expr_type)
            }
            ConversionError::UnsupportedLiteral(literal_type) => {
                write!(f, "Unsupported literal type: {}", literal_type)
            }
            ConversionError::UnsupportedOperator(op) => {
                write!(f, "Unsupported operator: {}", op)
            }
            ConversionError::InternalError(msg) => {
                write!(f, "Internal error during SQL conversion: {}", msg)
            }
        }
    }
}

impl std::error::Error for ConversionError {}

/// Converts a DataFusion logical `Expr` to an SQL WHERE clause string.
/// This function supports a basic subset of expressions needed for simple predicates.
pub fn expr_to_sql(expr: &Expr) -> Result<String, ConversionError> {
    match expr {
        Expr::Column(col) => {
            // For simplicity, not handling complex/qualified column names fully here.
            // Assuming simple column names that don't need extensive quoting.
            // DataFusion's `Column::flat_name()` provides a string representation.
            Ok(format!("\"{}\"", col.flat_name()))
        }
        Expr::Literal(scalar_value) => literal_to_sql(scalar_value),
        Expr::BinaryExpr(binary_expr) => {
            let left = expr_to_sql(&binary_expr.left)?;
            let right = expr_to_sql(&binary_expr.right)?;
            let op = match binary_expr.op {
                datafusion::logical_expr::Operator::Eq => "=",
                datafusion::logical_expr::Operator::NotEq => "!=",
                datafusion::logical_expr::Operator::Lt => "<",
                datafusion::logical_expr::Operator::LtEq => "<=",
                datafusion::logical_expr::Operator::Gt => ">",
                datafusion::logical_expr::Operator::GtEq => ">=",
                datafusion::logical_expr::Operator::And => "AND",
                datafusion::logical_expr::Operator::Or => "OR",
                // Add other operators as needed
                _ => return Err(ConversionError::UnsupportedOperator(format!("{:?}", binary_expr.op))),
            };
            Ok(format!("({}) {} ({})", left, op, right))
        }
        Expr::IsNull(expr) => {
            let inner_sql = expr_to_sql(expr)?;
            Ok(format!("({}) IS NULL", inner_sql))
        }
        Expr::IsNotNull(expr) => {
            let inner_sql = expr_to_sql(expr)?;
            Ok(format!("({}) IS NOT NULL", inner_sql))
        }
        // TODO: Add support for Expr::InList if needed.
        // Expr::InList { expr, list, negated } => { ... }

        // Fallback for unsupported expressions
        _ => Err(ConversionError::UnsupportedExpression(format!("{:?}", expr))),
    }
}

fn literal_to_sql(scalar_value: &ScalarValue) -> Result<String, ConversionError> {
    match scalar_value {
        ScalarValue::Boolean(Some(b)) => Ok(if *b { "TRUE" } else { "FALSE" }.to_string()),
        ScalarValue::Float32(Some(f)) => Ok(f.to_string()),
        ScalarValue::Float64(Some(f)) => Ok(f.to_string()),
        ScalarValue::Int8(Some(i)) => Ok(i.to_string()),
        ScalarValue::Int16(Some(i)) => Ok(i.to_string()),
        ScalarValue::Int32(Some(i)) => Ok(i.to_string()),
        ScalarValue::Int64(Some(i)) => Ok(i.to_string()),
        ScalarValue::UInt8(Some(i)) => Ok(i.to_string()),
        ScalarValue::UInt16(Some(i)) => Ok(i.to_string()),
        ScalarValue::UInt32(Some(i)) => Ok(i.to_string()),
        ScalarValue::UInt64(Some(i)) => Ok(i.to_string()),
        ScalarValue::Utf8(Some(s)) => Ok(format!("'{}'", s.replace('\'', "''"))), // Escape single quotes
        ScalarValue::LargeUtf8(Some(s)) => Ok(format!("'{}'", s.replace('\'', "''"))),
        ScalarValue::TimestampNanosecond(Some(nanos), tz_opt) |
        ScalarValue::TimestampMicrosecond(Some(nanos), tz_opt) | // Convert to nanos for chrono
        ScalarValue::TimestampMillisecond(Some(nanos), tz_opt) | // Convert to nanos for chrono
        ScalarValue::TimestampSecond(Some(nanos), tz_opt) => { // Convert to nanos for chrono
            let true_nanos = match scalar_value {
                ScalarValue::TimestampMicrosecond(_, _) => *nanos * 1_000,
                ScalarValue::TimestampMillisecond(_, _) => *nanos * 1_000_000,
                ScalarValue::TimestampSecond(_, _) => *nanos * 1_000_000_000,
                _ => *nanos,
            };
            // Assumes the nanoseconds are relative to UTC if tz_opt is None or UTC.
            // If tz_opt contains a specific timezone, chrono can handle it.
            match tz_opt {
                Some(tz_str) => {
                     match tz_str.parse::<chrono::FixedOffset>() {
                        Ok(tz) => Ok(format!("TIMESTAMP '{}'", chrono::DateTime::<chrono::FixedOffset>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(true_nanos / 1_000_000_000, (true_nanos % 1_000_000_000) as u32).unwrap_or_default(), tz).to_rfc3339())),
                        Err(_) => match tz_str.parse::<chrono_tz::Tz>() {
                            Ok(tz) => Ok(format!("TIMESTAMP '{}'", tz.from_utc_datetime(&chrono::NaiveDateTime::from_timestamp_opt(true_nanos / 1_000_000_000, (true_nanos % 1_000_000_000) as u32).unwrap_or_default()).to_rfc3339())),
                            Err(_) => Err(ConversionError::UnsupportedLiteral(format!("Unsupported timezone string: {}", tz_str)))
                        }
                     }
                }
                None => { // Assume UTC
                    let dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(true_nanos / 1_000_000_000, (true_nanos % 1_000_000_000) as u32).unwrap_or_default(), chrono::Utc);
                    Ok(format!("TIMESTAMP '{}'", dt.to_rfc3339()))
                }
            }
        }
        ScalarValue::Null => Ok("NULL".to_string()),
        _ => Err(ConversionError::UnsupportedLiteral(format!("{:?}", scalar_value))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit, lit_timestamp_nano, Expr};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    fn C(name: &str) -> Expr { col(name) }
    fn S(val: &str) -> Expr { lit(val) }
    fn I(val: i64) -> Expr { lit(val) }
    fn F(val: f64) -> Expr { lit(val) }
    fn B(val: bool) -> Expr { lit(val) }
    fn T(val: i64) -> Expr { lit_timestamp_nano(val) } // Creates UTC timestamp

    #[test]
    fn test_simple_column() {
        assert_eq!(expr_to_sql(&C("my_col")).unwrap(), "\"my_col\"");
    }

    #[test]
    fn test_literals() {
        assert_eq!(literal_to_sql(&ScalarValue::Int32(Some(123))).unwrap(), "123");
        assert_eq!(literal_to_sql(&ScalarValue::Utf8(Some("hello".to_string()))).unwrap(), "'hello'");
        assert_eq!(literal_to_sql(&ScalarValue::Utf8(Some("hello 'world'".to_string()))).unwrap(), "'hello ''world'''");
        assert_eq!(literal_to_sql(&ScalarValue::Boolean(Some(true))).unwrap(), "TRUE");
        assert_eq!(literal_to_sql(&ScalarValue::Boolean(Some(false))).unwrap(), "FALSE");
        assert_eq!(literal_to_sql(&ScalarValue::Float64(Some(1.23))).unwrap(), "1.23");
        assert_eq!(literal_to_sql(&ScalarValue::Null).unwrap(), "NULL");
    }

    #[test]
    fn test_timestamp_literal_utc() {
        // Equivalent to 1970-01-01T00:00:00.000000123Z
        let dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(0, 123).unwrap(), chrono::Utc);
        let rfc3339_time = dt.to_rfc3339();
        assert_eq!(
            literal_to_sql(&ScalarValue::TimestampNanosecond(Some(123), None)).unwrap(),
            format!("TIMESTAMP '{}'", rfc3339_time)
        );
    }

    #[test]
    fn test_timestamp_literal_with_offset() {
        // 123 ns, with +01:00 offset
        let nanos = 123;
        let offset_seconds = 3600; // +01:00
        let fixed_offset = chrono::FixedOffset::east_opt(offset_seconds).unwrap();
        let dt = chrono::DateTime::<chrono::FixedOffset>::from_naive_utc_and_offset(chrono::NaiveDateTime::from_timestamp_opt(0, nanos).unwrap(), fixed_offset);
        let rfc3339_time = dt.to_rfc3339();

        assert_eq!(
            literal_to_sql(&ScalarValue::TimestampNanosecond(Some(nanos as i64), Some(Arc::new(fixed_offset.to_string())) )).unwrap(),
            format!("TIMESTAMP '{}'", rfc3339_time)
        );
    }


    #[test]
    fn test_simple_binary_expr() {
        assert_eq!(expr_to_sql(&C("age").gt(I(30))).unwrap(), "(\"age\") > (30)");
        assert_eq!(expr_to_sql(&C("name").eq(S("Alice"))).unwrap(), "(\"name\") = ('Alice')");
    }

    #[test]
    fn test_compound_binary_expr() {
        let expr = C("age").gt(I(30)).and(C("city").eq(S("New York")));
        assert_eq!(expr_to_sql(&expr).unwrap(), "((\"age\") > (30)) AND ((\"city\") = ('New York'))");
    }

    #[test]
    fn test_is_null_is_not_null() {
        assert_eq!(expr_to_sql(&C("value").is_null()).unwrap(), "(\"value\") IS NULL");
        assert_eq!(expr_to_sql(&C("value").is_not_null()).unwrap(), "(\"value\") IS NOT NULL");
    }

    #[test]
    fn test_unsupported_expr() {
        let expr = Expr::Exists { subquery: Arc::new(DFLogicalPlan::empty(true)), negated: false }; // DFLogicalPlan needs to be imported
        assert!(expr_to_sql(&expr).is_err());
    }
}
