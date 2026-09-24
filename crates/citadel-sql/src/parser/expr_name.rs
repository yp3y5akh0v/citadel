//! Stable display names for expression projections.

use super::{BinOp, Expr};

pub(crate) fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::QualifiedColumn { table, column } => format!("{table}.{column}"),
        Expr::Literal(v) => format!("{v}"),
        Expr::CountStar => "COUNT(*)".into(),
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            let arg_strs: Vec<String> = args.iter().map(expr_display_name).collect();
            if *distinct {
                format!("{name}(DISTINCT {})", arg_strs.join(", "))
            } else {
                format!("{name}({})", arg_strs.join(", "))
            }
        }
        Expr::BinaryOp { left, op, right } => {
            format!(
                "{} {} {}",
                expr_display_name(left),
                op_symbol(op),
                expr_display_name(right)
            )
        }
        Expr::WindowFunction { name, args, .. } => {
            if args.is_empty() {
                format!("{name}()")
            } else {
                let arg_strs: Vec<String> = args.iter().map(expr_display_name).collect();
                format!("{name}({})", arg_strs.join(", "))
            }
        }
        _ => "?".into(),
    }
}

pub(crate) fn op_symbol(op: &BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Eq => "=",
        BinOp::NotEq => "<>",
        BinOp::Lt => "<",
        BinOp::Gt => ">",
        BinOp::LtEq => "<=",
        BinOp::GtEq => ">=",
        BinOp::And => "AND",
        BinOp::Or => "OR",
        BinOp::Concat => "||",
        BinOp::JsonGet => "->",
        BinOp::JsonGetText => "->>",
        BinOp::JsonPath => "#>",
        BinOp::JsonPathText => "#>>",
        BinOp::JsonContains => "@>",
        BinOp::JsonContainedBy => "<@",
        BinOp::JsonHasKey => "?",
        BinOp::JsonHasAnyKey => "?|",
        BinOp::JsonHasAllKeys => "?&",
        BinOp::JsonDeletePath => "#-",
        BinOp::JsonPathExists => "@?",
        BinOp::JsonPathMatch => "@@",
        BinOp::JsonPathExistsTz => "@?_tz",
        BinOp::JsonPathMatchTz => "@@_tz",
        BinOp::VectorL2 => "<->",
        BinOp::VectorInner => "<#>",
        BinOp::VectorCosine => "<=>",
    }
}
