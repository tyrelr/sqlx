use proptest::prelude::*;
use sqlx::TypeInfo;
use sqlx::{sqlite::Sqlite, Column, Executor};
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
            ColType::Real => "FLOAT",
            ColType::Text => "TEXT",
            ColType::Blob => "BLOB",
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct ColumnModel {
    pub name: &'static str,
    pub col_type: ColType,
    pub nullable: bool,
}

fn my_tweet_column_strategy() -> impl Strategy<Value = ColumnModel> {
    prop_oneof![
        Just(ColumnModel {
            name: "id",
            col_type: ColType::Integer,
            nullable: false
        }),
        Just(ColumnModel {
            name: "text",
            col_type: ColType::Text,
            nullable: false
        }),
        Just(ColumnModel {
            name: "is_sent",
            col_type: ColType::Boolean,
            nullable: false
        }),
        Just(ColumnModel {
            name: "owner_id",
            col_type: ColType::Integer,
            nullable: true
        })
    ]
}

fn my_accounts_column_strategy() -> impl Strategy<Value = ColumnModel> {
    prop_oneof![
        Just(ColumnModel {
            name: "id",
            col_type: ColType::Integer,
            nullable: false
        }),
        Just(ColumnModel {
            name: "name",
            col_type: ColType::Text,
            nullable: false
        }),
        Just(ColumnModel {
            name: "is_active",
            col_type: ColType::Boolean,
            nullable: true
        })
    ]
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct TableModel {
    name: &'static str,
    alias: Option<String>,
}
impl TableModel {
    fn as_sql_code(&self) -> String {
        if let Some(alias) = &self.alias {
            format!("{} {}", self.name, alias)
        } else {
            self.name.to_string()
        }
    }

    fn as_sql_name(&self) -> String {
        if let Some(alias) = &self.alias {
            alias.to_string()
        } else {
            self.name.to_string()
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone, Copy)]
enum InfixNumericOperation {
    Add,
    Sub,
    Mul,
    Div,
}
impl InfixNumericOperation {
    fn as_sql_code(self) -> &'static str {
        match self {
            InfixNumericOperation::Add => "+",
            InfixNumericOperation::Sub => "-",
            InfixNumericOperation::Mul => "*",
            InfixNumericOperation::Div => "/",
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
enum ExpressionModel {
    ColumnProjection {
        table_alias: Option<String>,
        column: ColumnModel,
    },
    InfixNumericOperation(
        InfixNumericOperation,
        Box<ExpressionModel>,
        Box<ExpressionModel>,
    ),
}

impl ExpressionModel {
    fn as_sql_code(&self) -> String {
        match self {
            Self::ColumnProjection {
                table_alias,
                column,
            } => {
                if let Some(table_alias) = table_alias {
                    format!("{}.{}", table_alias, column.name)
                } else {
                    column.name.to_string()
                }
            }
            Self::InfixNumericOperation(op, left, right) => {
                format!(
                    "({} {} {})",
                    left.as_sql_code(),
                    op.as_sql_code(),
                    right.as_sql_code()
                )
            }
        }
    }

    fn output_name(&self) -> Option<&'static str> {
        match self {
            Self::ColumnProjection { column, .. } => Some(column.name),
            Self::InfixNumericOperation(..) => None,
        }
    }

    fn output_type(&self) -> ColType {
        match self {
            Self::ColumnProjection { column, .. } => column.col_type,
            Self::InfixNumericOperation(_, left, right) => {
                match (left.output_type(), right.output_type()) {
                    (ColType::Real, _) => ColType::Real,
                    (ColType::Integer, ColType::Real) => ColType::Real,
                    _ => ColType::Integer,
                }
            }
        }
    }

    fn output_nullable(&self) -> bool {
        match self {
            Self::ColumnProjection { column, .. } => column.nullable,
            Self::InfixNumericOperation(_, left, right) => {
                left.output_nullable() || right.output_nullable()
            }
        }
    }
}

fn my_expression_model_strategy<C: 'static + Strategy<Value = ColumnModel>>(
    table: TableModel,
    column_strategy: C,
) -> impl Strategy<Value = ExpressionModel> {
    let leaf = (
        prop_oneof![Just(None), Just(Some(table.as_sql_name()))],
        column_strategy,
    )
        .prop_map(
            move |(table_alias, column)| ExpressionModel::ColumnProjection {
                table_alias,
                column,
            },
        );

    leaf.prop_recursive(5, 15, 10, move |inner| {
        (
            prop_oneof![
                Just(InfixNumericOperation::Add),
                Just(InfixNumericOperation::Sub),
                Just(InfixNumericOperation::Mul),
                //Just(InfixNumericOperation::Div) //TODO: dividing by strings acts weird. ex: SELECT 1/"a"; -> null
            ],
            inner.clone(),
            inner.clone(),
        )
            .prop_map(move |(op, i1, i2)| {
                ExpressionModel::InfixNumericOperation(op, i1.into(), i2.into())
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
                alias: None
            }),
            my_tweet_column_strategy()
        ),
        my_query_strategy_args(
            Just(TableModel {
                name: "accounts",
                alias: Some("accounts".to_string())
            }),
            my_accounts_column_strategy()
        ),
        my_query_strategy_args(
            Just(TableModel {
                name: "accounts_view",
                alias: None
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
            let info = conn.describe(
                &format!(
                "SELECT {} FROM {}",
                query_model.projections.iter().map(|c|c.as_sql_code()).collect::<Vec<_>>().join(","),
                &query_model.table.as_sql_code()
            )).await.unwrap();

            let columns = info.columns();

            for (i,expr_model) in query_model.projections.iter().enumerate()
            {
                if expr_model.output_name().is_some()
                {
                    prop_assert_eq!(Some(columns[i].name()), expr_model.output_name());
                }
                prop_assert_eq!(info.nullable(i), Some(expr_model.output_nullable()));
                prop_assert_eq!(columns[i].type_info().name(), expr_model.output_type().name());
            }

            Ok(())
        });
        res.unwrap()
    }
}
