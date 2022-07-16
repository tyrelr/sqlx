use proptest::prelude::*;
use sqlx::TypeInfo;
use sqlx::{sqlite::Sqlite, Column, Executor};
use sqlx_test::new;

#[derive(Debug, PartialEq, Hash, Clone)]
struct ColumnModel {
    pub name: &'static str,
    pub typename: &'static str,
    pub nullable: bool,
}

fn my_tweet_column_strategy() -> impl Strategy<Value = ColumnModel> {
    prop_oneof![
        Just(ColumnModel {
            name: "id",
            typename: "INTEGER",
            nullable: false
        }),
        Just(ColumnModel {
            name: "text",
            typename: "TEXT",
            nullable: false
        }),
        Just(ColumnModel {
            name: "is_sent",
            typename: "BOOLEAN",
            nullable: false
        }),
        Just(ColumnModel {
            name: "owner_id",
            typename: "INTEGER",
            nullable: true
        })
    ]
}

fn my_accounts_column_strategy() -> impl Strategy<Value = ColumnModel> {
    prop_oneof![
        Just(ColumnModel {
            name: "id",
            typename: "INTEGER",
            nullable: false
        }),
        Just(ColumnModel {
            name: "name",
            typename: "TEXT",
            nullable: false
        }),
        Just(ColumnModel {
            name: "is_active",
            typename: "BOOLEAN",
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

#[derive(Debug, PartialEq, Hash, Clone)]
enum ExpressionModel {
    ColumnProjection {
        table_alias: Option<String>,
        column: ColumnModel,
    },
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
        }
    }

    fn output_name(&self) -> &'static str {
        match self {
            Self::ColumnProjection { column, .. } => column.name,
        }
    }

    fn output_typename(&self) -> &'static str {
        match self {
            Self::ColumnProjection { column, .. } => column.typename,
        }
    }

    fn output_nullable(&self) -> bool {
        match self {
            Self::ColumnProjection { column, .. } => column.nullable,
        }
    }
}

fn my_expression_model_strategy<C: Strategy<Value = ColumnModel>>(
    table: TableModel,
    column_strategy: C,
) -> impl Strategy<Value = ExpressionModel> {
    (
        prop_oneof![Just(None), Just(Some(table.as_sql_name()))],
        column_strategy,
    )
        .prop_map(
            move |(table_alias, column)| ExpressionModel::ColumnProjection {
                table_alias,
                column,
            },
        )
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct QueryModel {
    pub projections: Vec<ExpressionModel>,
    pub table: TableModel,
}

fn my_query_strategy_args<T: Strategy<Value = TableModel>, C: Strategy<Value = ColumnModel>>(
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
                prop_assert_eq!(columns[i].name(), expr_model.output_name());
                prop_assert_eq!(info.nullable(i), Some(expr_model.output_nullable()));
                prop_assert_eq!(columns[i].type_info().name(), expr_model.output_typename());
            }

            Ok(())
        });
        res.unwrap()
    }
}
