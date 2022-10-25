use proptest::prelude::*;
use sqlx::TypeInfo;
use sqlx::{sqlite::Sqlite, Column, Executor, Row, ValueRef};
use sqlx_test::new;

//TODO: find the right balance between sqlite types vs. high-level types
#[derive(Debug, PartialEq, Hash, Clone, Copy)]
enum ColType {
    Boolean,
    Integer,
    Real,
    Text,
    Blob,
}

impl ColType {
    fn name(self) -> &'static str {
        match self {
            ColType::Boolean => "BOOLEAN",
            ColType::Integer => "INTEGER",
            ColType::Real => "REAL",
            ColType::Text => "TEXT",
            ColType::Blob => "BLOB",
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct ColumnInfo {
    pub name: Option<&'static str>,
    pub col_type: ColType,
    pub nullable: bool,
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct ColumnModel {
    pub name: &'static str,
    pub col_type: ColType,
    pub nullable: bool,
}

impl ColumnModel {
    fn as_sql_code(&self) -> String {
        self.name.to_string()
    }

    fn to_column_info(&self) -> ColumnInfo {
        ColumnInfo {
            name: Some(self.name),
            col_type: self.col_type,
            nullable: self.nullable,
        }
    }
}

fn my_tweet_column_strategy() -> impl Strategy<Value = ColumnModel> {
    prop_oneof![
        Just(ColumnModel {
            name: "id".into(),
            col_type: ColType::Integer,
            nullable: false
        }),
        Just(ColumnModel {
            name: "text".into(),
            col_type: ColType::Text,
            nullable: false
        }),
        Just(ColumnModel {
            name: "is_sent".into(),
            col_type: ColType::Boolean,
            nullable: false
        }),
        Just(ColumnModel {
            name: "owner_id".into(),
            col_type: ColType::Integer,
            nullable: true
        })
    ]
}

fn my_accounts_column_strategy() -> impl Strategy<Value = ColumnModel> {
    prop_oneof![
        Just(ColumnModel {
            name: "id".into(),
            col_type: ColType::Integer,
            nullable: false
        }),
        Just(ColumnModel {
            name: "name".into(),
            col_type: ColType::Text,
            nullable: false
        }),
        Just(ColumnModel {
            name: "is_active".into(),
            col_type: ColType::Boolean,
            nullable: true
        })
    ]
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct TableModel {
    name: &'static str,
    output_alias: Option<String>,
}
impl TableModel {
    fn as_sql_code(&self) -> String {
        if let Some(output_alias) = &self.output_alias {
            format!("{} {}", self.name, output_alias)
        } else {
            self.name.to_string()
        }
    }

    fn as_sql_name(&self) -> String {
        if let Some(output_alias) = &self.output_alias {
            output_alias.to_string()
        } else {
            self.name.to_string()
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
enum LiteralValue {
    Boolean,
    Integer,
    Real,
    Text,
    Blob,
}
impl LiteralValue {
    fn as_sql_code(&self) -> String {
        match self {
            Self::Boolean => "true",
            Self::Integer => "1",
            Self::Real => "1.0",
            Self::Text => "\'\'",
            Self::Blob => "x\'\'",
        }
        .to_string()
    }

    fn output_column_info(&self) -> ColumnInfo {
        ColumnInfo {
            name: None, //TODO: check literal expression name too
            col_type: match self {
                Self::Boolean => ColType::Integer,
                Self::Integer => ColType::Integer,
                Self::Real => ColType::Real,
                Self::Text => ColType::Text,
                Self::Blob => ColType::Blob,
            },
            nullable: false,
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct ColumnProjection {
    source_alias: String,
    column: ColumnModel,
}
impl ColumnProjection {
    fn as_sql_code(&self) -> String {
        format!("{}.{}", self.source_alias, self.column.as_sql_code())
    }

    fn output_column_info(&self) -> ColumnInfo {
        self.column.to_column_info()
    }
}

#[derive(Debug, PartialEq, Hash, Clone, Copy)]
enum InfixNumericOperationType {
    Add,
    Sub,
    Mul,
    Div,
}
impl InfixNumericOperationType {
    fn as_sql_code(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
        }
    }
    fn output_column_type(&self, left: &ColumnInfo, right: &ColumnInfo) -> ColType {
        match self {
            Self::Add | Self::Sub | Self::Mul | Self::Div => {
                match (left.col_type, right.col_type) {
                    (ColType::Real, _) => ColType::Real,
                    (_, ColType::Real) => ColType::Real,
                    _ => ColType::Integer,
                }
            }
        }
    }
    fn output_nullable(&self, left: &ColumnInfo, right: &ColumnInfo) -> bool {
        match self {
            Self::Add | Self::Sub | Self::Mul => left.nullable || right.nullable,
            Self::Div => {
                left.nullable
                    || right.nullable
                    || right.col_type == ColType::Text
                    || right.col_type == ColType::Blob
            } //dividing by string or blob gives null
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct InfixNumericOperation {
    operation: InfixNumericOperationType,
    left: ExpressionModel,
    right: ExpressionModel,
}

impl InfixNumericOperation {
    fn as_sql_code(&self) -> String {
        format!(
            "({} {} {})",
            self.left.as_sql_code(),
            self.operation.as_sql_code(),
            self.right.as_sql_code()
        )
    }
    fn output_column_info(&self) -> ColumnInfo {
        ColumnInfo {
            name: None,
            col_type: self.operation.output_column_type(
                &self.left.output_column_info(),
                &self.right.output_column_info(),
            ),
            nullable: self.operation.output_nullable(
                &self.left.output_column_info(),
                &self.right.output_column_info(),
            ),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
enum ExpressionModel {
    LiteralValue(LiteralValue),
    ColumnProjection(ColumnProjection),
    InfixNumericOperation(Box<InfixNumericOperation>),
}

impl ExpressionModel {
    fn as_sql_code(&self) -> String {
        match self {
            Self::LiteralValue(lit) => lit.as_sql_code(),
            Self::ColumnProjection(col) => col.as_sql_code(),
            Self::InfixNumericOperation(op) => op.as_sql_code(),
        }
    }

    fn output_column_info(&self) -> ColumnInfo {
        match self {
            Self::LiteralValue(lit) => lit.output_column_info(),
            Self::ColumnProjection(col) => col.output_column_info(),
            Self::InfixNumericOperation(op) => op.output_column_info(),
        }
    }
}

fn my_expression_model_strategy<C: 'static + Strategy<Value = ColumnModel>>(
    table: TableModel,
    column_strategy: C,
) -> impl Strategy<Value = ExpressionModel> {
    let table_projection = (prop_oneof![Just(table.as_sql_name())], column_strategy).prop_map(
        move |(source_alias, column)| {
            ExpressionModel::ColumnProjection(ColumnProjection {
                source_alias,
                column,
            })
        },
    );

    let literal_projection = prop_oneof![
        Just(ExpressionModel::LiteralValue(LiteralValue::Boolean)),
        Just(ExpressionModel::LiteralValue(LiteralValue::Integer)),
        Just(ExpressionModel::LiteralValue(LiteralValue::Real)),
        Just(ExpressionModel::LiteralValue(LiteralValue::Text)),
        Just(ExpressionModel::LiteralValue(LiteralValue::Blob)),
    ];

    let leaf = prop_oneof![literal_projection, table_projection];

    leaf.prop_recursive(5, 15, 2, move |inner| {
        (
            prop_oneof![
                Just(InfixNumericOperationType::Add),
                Just(InfixNumericOperationType::Sub),
                Just(InfixNumericOperationType::Mul),
                //Just(InfixNumericOperationType::Div) //TODO: dividing by strings acts weird, sqlx doesn't implement that. ex: SELECT 1/"a"; -> null
            ],
            inner.clone(),
            inner.clone(),
        )
            .prop_map(move |(operation, expr1, expr2)| {
                ExpressionModel::InfixNumericOperation(
                    InfixNumericOperation {
                        operation,
                        left: expr1,
                        right: expr2,
                    }
                    .into(),
                )
            })
    })
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct QueryModel {
    pub projections: Vec<ExpressionModel>,
    pub table: TableModel,
}

fn my_query_strategy_args<
    T: 'static + Strategy<Value = TableModel>,
    C: 'static + Strategy<Value = ColumnModel>,
>(
    table_strategy: T,
    column_strategy: C,
) -> impl Strategy<Value = QueryModel> {
    let column_strategy = std::sync::Arc::new(column_strategy);
    table_strategy.prop_flat_map(move |table| {
        let columns_strategy = prop::collection::vec(
            my_expression_model_strategy(table.clone(), column_strategy.clone()),
            1..100,
        );
        columns_strategy.prop_map(move |projections| QueryModel {
            projections,
            table: table.clone(),
        })
    })
}

fn my_query_strategy() -> impl Strategy<Value = QueryModel> {
    prop_oneof![
        my_query_strategy_args(
            Just(TableModel {
                name: "tweet",
                output_alias: None
            }),
            my_tweet_column_strategy()
        ),
        my_query_strategy_args(
            Just(TableModel {
                name: "accounts",
                output_alias: Some("accounts".to_string())
            }),
            my_accounts_column_strategy()
        ),
        my_query_strategy_args(
            Just(TableModel {
                name: "accounts_view",
                output_alias: None
            }),
            my_accounts_column_strategy()
        ),
    ]
}

proptest! {
    #[test]
    fn describe_query_prop_test(query_model in my_query_strategy()) {
        eprintln!("{:?}",query_model);
        let res = ::sqlx_rt::async_std::task::block_on(async{
            let mut conn = new::<Sqlite>().await.unwrap();
            {
                let info = conn.describe(
                    &dbg!(format!(
                    "SELECT {} FROM {}",
                    query_model.projections.iter().map(|c|c.as_sql_code()).collect::<Vec<_>>().join(","),
                    &query_model.table.as_sql_code()
                ))).await.unwrap();

                let columns = info.columns();

                for (i,expr_model) in query_model.projections.iter().enumerate()
                {
                    let expected_column = expr_model.output_column_info();

                    if let Some(expected_column_name) = expected_column.name
                    {
                        prop_assert_eq!(&columns[i].name(), &expected_column_name);
                    }
                    prop_assert_eq!(info.nullable(i), Some(expected_column.nullable));
                    prop_assert_eq!(columns[i].type_info().name(), expected_column.col_type.name());
                }
            }

            let rows = conn.fetch_all(
                &*dbg!(format!(
                "SELECT DISTINCT {} FROM {} LIMIT 100",
                query_model.projections.iter().map(|c|c.as_sql_code()).collect::<Vec<_>>().join(","),
                &query_model.table.as_sql_code()
            ))).await.unwrap();

            for row in rows
            {
                let columns = row.columns();

                for (i,expr_model) in query_model.projections.iter().enumerate()
                {
                   let expected_column = expr_model.output_column_info();
                   let actual_value = row.try_get_raw(i)?;
                   let actual_type_info = actual_value.type_info();

                   if actual_type_info.name() == "NULL" {
                       prop_assert_eq!(true, expected_column.nullable);
                   }
                   else if actual_type_info.name() == "INTEGER" && expected_column.col_type.name() == "BOOLEAN"
                   {
                       //this is acceptable, sqlite uses integers for boolean types
                   }
                   else
                   {
                       prop_assert_eq!(actual_type_info.name(), expected_column.col_type.name());
                   }
                }
            }

            Ok(())
        });
        res.unwrap()
    }
}
