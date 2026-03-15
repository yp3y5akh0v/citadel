use citadel_sql::{ExecutionResult, QueryResult, Value};

use crate::repl::Settings;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OutputMode {
    Box,
    Table,
    Csv,
    Json,
    Line,
}

impl OutputMode {
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s {
            "box" => Some(Self::Box),
            "table" => Some(Self::Table),
            "csv" => Some(Self::Csv),
            "json" => Some(Self::Json),
            "line" => Some(Self::Line),
            _ => None,
        }
    }
}

impl std::fmt::Display for OutputMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Box => write!(f, "box"),
            Self::Table => write!(f, "table"),
            Self::Csv => write!(f, "csv"),
            Self::Json => write!(f, "json"),
            Self::Line => write!(f, "line"),
        }
    }
}

pub fn format_result(result: &ExecutionResult, settings: &Settings) -> String {
    match result {
        ExecutionResult::Query(qr) => format_query(qr, settings),
        ExecutionResult::RowsAffected(n) => {
            if settings.show_changes {
                format!("{n} row(s) affected")
            } else {
                String::new()
            }
        }
        ExecutionResult::Ok => String::new(),
    }
}

pub fn format_query(qr: &QueryResult, settings: &Settings) -> String {
    if qr.rows.is_empty() && qr.columns.is_empty() {
        return String::new();
    }

    let result = match settings.mode {
        OutputMode::Box => format_box(qr, settings),
        OutputMode::Table => format_table(qr, settings),
        OutputMode::Csv => format_csv(qr, settings),
        OutputMode::Json => format_json(qr, settings),
        OutputMode::Line => format_line(qr, settings),
    };

    let count = qr.rows.len();
    if matches!(settings.mode, OutputMode::Box | OutputMode::Table) {
        format!("{result}\n({count} row{})", if count == 1 { "" } else { "s" })
    } else {
        result
    }
}

fn value_to_string(v: &Value, settings: &Settings) -> String {
    match v {
        Value::Null => settings.null_display.clone(),
        Value::Integer(n) => n.to_string(),
        Value::Real(r) => r.to_string(),
        Value::Text(s) => s.clone(),
        Value::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Blob(b) => {
            let mut s = String::with_capacity(2 + b.len() * 2);
            s.push_str("X'");
            for byte in b {
                s.push_str(&format!("{byte:02X}"));
            }
            s.push('\'');
            s
        }
    }
}

fn apply_column_widths(table: &mut comfy_table::Table, settings: &Settings) {
    use comfy_table::{ColumnConstraint, Width};
    for (i, &w) in settings.column_widths.iter().enumerate() {
        if w > 0 {
            if let Some(col) = table.column_mut(i) {
                col.set_constraint(ColumnConstraint::Absolute(Width::Fixed(w as u16)));
            }
        }
    }
}

fn format_box(qr: &QueryResult, settings: &Settings) -> String {
    use comfy_table::{ContentArrangement, Table, presets::UTF8_FULL};

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);

    if settings.show_headers && !qr.columns.is_empty() {
        table.set_header(&qr.columns);
    }

    for row in &qr.rows {
        let cells: Vec<String> = row.iter().map(|v| value_to_string(v, settings)).collect();
        table.add_row(cells);
    }

    if !settings.column_widths.is_empty() {
        apply_column_widths(&mut table, settings);
    }

    table.to_string()
}

fn format_table(qr: &QueryResult, settings: &Settings) -> String {
    use comfy_table::{ContentArrangement, Table, presets::ASCII_FULL};

    let mut table = Table::new();
    table.load_preset(ASCII_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);

    if settings.show_headers && !qr.columns.is_empty() {
        table.set_header(&qr.columns);
    }

    for row in &qr.rows {
        let cells: Vec<String> = row.iter().map(|v| value_to_string(v, settings)).collect();
        table.add_row(cells);
    }

    if !settings.column_widths.is_empty() {
        apply_column_widths(&mut table, settings);
    }

    table.to_string()
}

fn format_csv(qr: &QueryResult, settings: &Settings) -> String {
    let mut out = String::new();

    if settings.show_headers && !qr.columns.is_empty() {
        let header_line: Vec<String> = qr.columns.iter().map(|c| csv_escape(c)).collect();
        out.push_str(&header_line.join(","));
        out.push('\n');
    }

    for row in &qr.rows {
        let cells: Vec<String> = row
            .iter()
            .map(|v| csv_escape(&value_to_string(v, settings)))
            .collect();
        out.push_str(&cells.join(","));
        out.push('\n');
    }

    if out.ends_with('\n') {
        out.pop();
    }
    out
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        let escaped = s.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        s.to_string()
    }
}

fn format_json(qr: &QueryResult, settings: &Settings) -> String {
    let mut rows = Vec::with_capacity(qr.rows.len());

    for row in &qr.rows {
        let mut obj = serde_json::Map::new();
        for (i, val) in row.iter().enumerate() {
            let col_name = qr
                .columns
                .get(i)
                .map(|s| s.as_str())
                .unwrap_or("?");
            let json_val = value_to_json(val, settings);
            obj.insert(col_name.to_string(), json_val);
        }
        rows.push(serde_json::Value::Object(obj));
    }

    serde_json::to_string_pretty(&rows).unwrap_or_else(|_| "[]".to_string())
}

fn value_to_json(v: &Value, _settings: &Settings) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Integer(n) => serde_json::Value::Number((*n).into()),
        Value::Real(r) => {
            serde_json::Number::from_f64(*r)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Blob(b) => {
            let mut hex = String::with_capacity(b.len() * 2);
            for byte in b {
                hex.push_str(&format!("{byte:02X}"));
            }
            let mut obj = serde_json::Map::new();
            obj.insert("$blob".to_string(), serde_json::Value::String(hex));
            serde_json::Value::Object(obj)
        }
    }
}

fn format_line(qr: &QueryResult, settings: &Settings) -> String {
    let mut out = String::new();

    let max_col_len = qr.columns.iter().map(|c| c.len()).max().unwrap_or(0);

    for (i, row) in qr.rows.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        for (j, val) in row.iter().enumerate() {
            let col = qr.columns.get(j).map(|s| s.as_str()).unwrap_or("?");
            let val_str = value_to_string(val, settings);
            out.push_str(&format!("{:>width$} = {val_str}\n", col, width = max_col_len));
        }
    }

    if out.ends_with('\n') {
        out.pop();
    }
    out
}
