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

#[derive(Debug, PartialEq, Hash, Clone)]
struct TableModel {
    name: &'static str,
    output_alias: Option<String>,
    columns: Vec<ColumnModel>,
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

fn my_tweet_table_info(output_alias: Option<String>) -> TableModel {
    TableModel {
        name: "tweet",
        output_alias,
        columns: vec![
            ColumnModel {
                name: "id".into(),
                col_type: ColType::Integer,
                nullable: false,
            },
            ColumnModel {
                name: "text".into(),
                col_type: ColType::Text,
                nullable: false,
            },
            ColumnModel {
                name: "is_sent".into(),
                col_type: ColType::Boolean,
                nullable: false,
            },
            ColumnModel {
                name: "owner_id".into(),
                col_type: ColType::Integer,
                nullable: true,
            },
        ],
    }
}

fn my_accounts_table_info(output_alias: Option<String>) -> TableModel {
    TableModel {
        name: "accounts",
        output_alias,
        columns: vec![
            ColumnModel {
                name: "id".into(),
                col_type: ColType::Integer,
                nullable: false,
            },
            ColumnModel {
                name: "name".into(),
                col_type: ColType::Text,
                nullable: false,
            },
            ColumnModel {
                name: "is_active".into(),
                col_type: ColType::Boolean,
                nullable: true,
            },
        ],
    }
}

fn my_accounts_view_table_info(output_alias: Option<String>) -> TableModel {
    TableModel {
        name: "accounts_view",
        output_alias: Some("accounts".to_string()),
        columns: vec![
            ColumnModel {
                name: "id".into(),
                col_type: ColType::Integer,
                nullable: false,
            },
            ColumnModel {
                name: "name".into(),
                col_type: ColType::Text,
                nullable: false,
            },
            ColumnModel {
                name: "is_active".into(),
                col_type: ColType::Boolean,
                nullable: true,
            },
        ],
    }
}

fn my_table_strategy() -> impl Strategy<Value = TableModel> {
    prop_oneof![
        Just(my_tweet_table_info(None)),
        Just(my_accounts_table_info(Some("accounts".to_string()))),
        Just(my_accounts_view_table_info(None)),
    ]
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

fn my_literal_expression_model_strategy() -> impl Strategy<Value = ExpressionModel> {
    prop_oneof![
        Just(ExpressionModel::LiteralValue(LiteralValue::Boolean)),
        Just(ExpressionModel::LiteralValue(LiteralValue::Integer)),
        Just(ExpressionModel::LiteralValue(LiteralValue::Real)),
        Just(ExpressionModel::LiteralValue(LiteralValue::Text)),
        Just(ExpressionModel::LiteralValue(LiteralValue::Blob)),
    ]
}

fn my_table_projection_expression_model_strategy(
    table: TableModel,
) -> impl Strategy<Value = ExpressionModel> {
    prop_oneof![my_literal_expression_model_strategy(), {
        let column_strategy = proptest::sample::select(table.columns.clone());
        column_strategy.prop_map(move |column: ColumnModel| {
            ExpressionModel::ColumnProjection(ColumnProjection {
                source_alias: table.as_sql_name(),
                column,
            })
        })
    }]
}

fn my_expression_tree_strategy<E: 'static + Strategy<Value = ExpressionModel>>(
    leaf_strategy: E,
) -> impl Strategy<Value = ExpressionModel> + Clone {
    leaf_strategy.prop_recursive(5, 15, 2, move |inner| {
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
enum NullOrderModel {
    Default,
    First,
    Last,
}

impl NullOrderModel {
    pub fn as_sql_code(&self) -> String {
        match self {
            Self::Default => "",
            Self::First => "NULLS FIRST",
            Self::Last => "NULLS LAST",
        }
        .to_string()
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
enum OrderModel {
    Default,
    Asc,
    Desc,
}

impl OrderModel {
    pub fn as_sql_code(&self) -> String {
        match self {
            Self::Default => "",
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
        .to_string()
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct OrderByExpressionModel {
    pub expression: ExpressionModel,
    pub order: OrderModel,
    pub null_order: NullOrderModel,
}

impl OrderByExpressionModel {
    pub fn as_sql_code(&self) -> String {
        format!(
            "{} {} {}",
            self.expression.as_sql_code(),
            self.order.as_sql_code(),
            self.null_order.as_sql_code()
        )
    }
}

fn my_orderby_expression_strategy<E: 'static + Strategy<Value = ExpressionModel>>(
    expression_strategy: E,
) -> impl Strategy<Value = OrderByExpressionModel> {
    let order_strategy = prop_oneof![
        Just(OrderModel::Default),
        Just(OrderModel::Asc),
        Just(OrderModel::Desc)
    ];
    let null_order_strategy = prop_oneof![
        Just(NullOrderModel::Default),
        Just(NullOrderModel::First),
        Just(NullOrderModel::Last)
    ];
    (expression_strategy, order_strategy, null_order_strategy).prop_map(
        |(expression, order, null_order)| OrderByExpressionModel {
            expression,
            order,
            null_order,
        },
    )
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct OrderByModel {
    pub expressions: Vec<OrderByExpressionModel>,
}

impl OrderByModel {
    pub fn as_sql_code(&self) -> String {
        if !self.expressions.is_empty() {
            format!(
                "ORDER BY {}",
                self.expressions
                    .iter()
                    .map(|c| c.as_sql_code())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        } else {
            "".to_string()
        }
    }
}

fn my_orderby_strategy<E: 'static + Strategy<Value = ExpressionModel>>(
    expression_strategy: E,
) -> impl Strategy<Value = OrderByModel> {
    let orderby_expression_model_strategy = my_orderby_expression_strategy(expression_strategy);
    let all_expressions_strategy = prop::collection::vec(orderby_expression_model_strategy, 0..10);
    all_expressions_strategy.prop_map(|expressions| OrderByModel { expressions })
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct QueryModel {
    pub projections: Vec<ExpressionModel>,
    pub table: Option<TableModel>,
    pub orderby: OrderByModel,
}

impl QueryModel {
    pub fn as_sql_code(&self) -> String {
        if let Some(table) = &self.table {
            format!(
                "SELECT {} FROM {} {}",
                self.projections
                    .iter()
                    .map(|c| c.as_sql_code())
                    .collect::<Vec<_>>()
                    .join(","),
                &table.as_sql_code(),
                self.orderby.as_sql_code()
            )
        } else {
            format!(
                "SELECT {} {}",
                self.projections
                    .iter()
                    .map(|c| c.as_sql_code())
                    .collect::<Vec<_>>()
                    .join(","),
                &self.orderby.as_sql_code()
            )
        }
    }

    pub fn as_sql_code_distinct_types(&self) -> String {
        if let Some(table) = &self.table {
            format!(
                "SELECT DISTINCT {} FROM {} {}",
                self.projections
                    .iter()
                    .map(|c| c.as_sql_code())
                    .collect::<Vec<_>>()
                    .join(","),
                &table.as_sql_code(),
                self.orderby.as_sql_code()
            )
        } else {
            format!(
                "SELECT {}",
                self.projections
                    .iter()
                    .map(|c| c.as_sql_code())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
    }
}

fn my_query_strategy_args<T: 'static + Strategy<Value = Option<TableModel>>>(
    table_strategy: T,
) -> impl Strategy<Value = QueryModel> {
    table_strategy.prop_flat_map(move |table| {
        let leaf_strategy: BoxedStrategy<ExpressionModel> = if let Some(table) = table.clone() {
            my_table_projection_expression_model_strategy(table.clone()).boxed()
        } else {
            my_literal_expression_model_strategy().boxed()
        };

        let expression_model_strategy = my_expression_tree_strategy(leaf_strategy);
        let select_columns_strategy =
            prop::collection::vec(expression_model_strategy.clone(), 1..10);
        let orderby_strategy = my_orderby_strategy(expression_model_strategy);

        (select_columns_strategy, orderby_strategy).prop_map(move |(projections, orderby)| {
            QueryModel {
                projections,
                table: table.clone(),
                orderby,
            }
        })
    })
}

fn my_query_strategy() -> impl Strategy<Value = QueryModel> {
    my_query_strategy_args(proptest::option::of(my_table_strategy()))
}

proptest! {
    #[test]
    fn describe_query_prop_test(query_model in my_query_strategy()) {
        eprintln!("{:?}",query_model);
        let res = ::sqlx_rt::async_std::task::block_on(async{
            let mut conn = new::<Sqlite>().await.unwrap();
            {
                let info = conn.describe(
                    &dbg!(query_model.as_sql_code())).await.unwrap();

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
                &*dbg!(query_model.as_sql_code_distinct_types())).await.unwrap();

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
