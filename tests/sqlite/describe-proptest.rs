use proptest::prelude::*;
use sqlx::TypeInfo;
use sqlx::{sqlite::Sqlite, Column, Executor, Row, ValueRef};
use sqlx_test::new;
use std::rc::Rc;

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

#[derive(Debug, Clone)]
struct ColumnInfo {
    pub name: Option<&'static str>,
    pub col_type: ColType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
struct ColumnModel {
    pub name: &'static str,
    pub col_type: ColType,
    pub nullable: bool,
}

impl ColumnModel {
    fn as_sql_code(&self) -> String {
        format!("\"{}\"", self.name)
    }

    fn to_column_info(&self) -> ColumnInfo {
        ColumnInfo {
            name: Some(self.name),
            col_type: self.col_type,
            nullable: self.nullable,
        }
    }
}

impl std::convert::TryFrom<&ColumnInfo> for ColumnModel {
    type Error = ();
    fn try_from(value: &ColumnInfo) -> Result<ColumnModel, ()> {
        if let Some(name) = value.name {
            Ok(ColumnModel {
                name,
                col_type: value.col_type,
                nullable: value.nullable,
            })
        } else {
            Err(())
        }
    }
}

trait TableContext: core::fmt::Debug {
    fn contains_alias(&self, alias: &str) -> bool;
}

impl TableContext for () {
    fn contains_alias(&self, alias: &str) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
struct TableModel {
    name: &'static str,
    columns: Vec<ColumnModel>,
}

impl TableModel {
    fn as_sql_code(&self) -> String {
        format!("\"{}\"", self.name.to_string())
    }
}

impl TableContext for TableModel {
    fn contains_alias(&self, alias: &str) -> bool {
        self.name == alias
    }
}

fn my_tweet_table_info() -> TableModel {
    TableModel {
        name: "tweet",
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

fn my_accounts_table_info() -> TableModel {
    TableModel {
        name: "accounts",
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

fn my_accounts_view_table_info() -> TableModel {
    TableModel {
        name: "accounts_view",
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

#[derive(Debug, Clone)]
struct OutputColumnInfo {
    pub output_alias: String,
    pub column_info: ColumnInfo,
}

#[derive(Debug, Clone)]
enum FromClauseLeafModel {
    Table {
        table: TableModel,
        output_alias: String,
    },
    Query {
        query: Box<QueryModel>,
        output_alias: String,
    },
}

impl FromClauseLeafModel {
    fn as_sql_code(&self) -> String {
        match self {
            Self::Table {
                table,
                output_alias,
            } => format!("{} \"{}\"", table.as_sql_code(), output_alias),
            Self::Query {
                query,
                output_alias,
            } => format!("({}) \"{}\"", query.as_sql_code(), output_alias),
        }
    }
    fn output_column_info(&self) -> Vec<OutputColumnInfo> {
        match self {
            Self::Table {
                table,
                output_alias,
            } => table
                .columns
                .iter()
                .cloned()
                .map(|c| OutputColumnInfo {
                    output_alias: output_alias.clone(),
                    column_info: c.to_column_info(),
                })
                .collect(),
            Self::Query {
                query,
                output_alias,
            } => query
                .output_column_info()
                .iter()
                .cloned()
                .map(|column_info| OutputColumnInfo {
                    output_alias: output_alias.clone(),
                    column_info,
                })
                .collect(),
        }
    }
}

impl TableContext for FromClauseLeafModel {
    fn contains_alias(&self, alias: &str) -> bool {
        match self {
            Self::Table { output_alias, .. } => output_alias == alias,
            Self::Query { output_alias, .. } => output_alias == alias,
        }
    }
}

fn unique_alias_strategy(table_context: Rc<dyn TableContext>) -> impl Strategy<Value = String> {
    proptest::string::string_regex("[A-Z]{1,3}")
        .unwrap()
        .prop_filter("alias must be unique", move |s| {
            !table_context.contains_alias(s)
        })
}

fn my_from_clause_leaf_strategy(
    table_context: Rc<dyn TableContext>,
    max_subquery_depth: u8,
) -> impl Strategy<Value = FromClauseLeafModel> + Clone {
    let table_strategy = prop_oneof![
        Just(my_tweet_table_info()),
        Just(my_accounts_table_info()),
        Just(my_accounts_view_table_info()),
    ];

    let alias_strategy = unique_alias_strategy(table_context.clone());
    let from_table_strategy =
        (alias_strategy, table_strategy).prop_map(move |(output_alias, table)| {
            FromClauseLeafModel::Table {
                table,
                output_alias,
            }
        });

    if max_subquery_depth == 0 {
        from_table_strategy.boxed()
    } else {
        let from_subquery_strategy = (
            my_query_strategy(table_context.clone(), max_subquery_depth - 1),
            unique_alias_strategy(table_context.clone()),
        )
            .prop_map(|(query, output_alias)| FromClauseLeafModel::Query {
                query: Box::new(query),
                output_alias,
            });

        prop_oneof![from_table_strategy, from_subquery_strategy].boxed()
    }
}

#[derive(Debug, Clone, Copy)]
enum JoinConditionModel {
    Cross,
}

impl JoinConditionModel {
    fn join_as_sql_code(&self) -> String {
        "CROSS JOIN".to_string()
    }
    fn condition_as_sql_code(&self) -> String {
        String::new()
    }
}

#[derive(Debug, Clone)]
enum FromJoinClauseModel {
    FromClause {
        table_context: Rc<dyn TableContext>,
        from: FromClauseLeafModel,
    },
    JoinClause {
        first: Rc<FromJoinClauseModel>,
        second: FromClauseLeafModel,
        join_condition: JoinConditionModel,
    },
}

impl FromJoinClauseModel {
    fn as_sql_code(&self) -> String {
        match self {
            Self::FromClause { from, .. } => format!("FROM {}", from.as_sql_code()),
            Self::JoinClause {
                first,
                second,
                join_condition,
            } => format!(
                "{} {} {} {}",
                first.as_sql_code(),
                join_condition.join_as_sql_code(),
                second.as_sql_code(),
                join_condition.condition_as_sql_code()
            ),
        }
    }
    fn output_column_info(&self) -> Vec<OutputColumnInfo> {
        match self {
            Self::FromClause { from, .. } => from.output_column_info(),
            Self::JoinClause { first, second, .. } => {
                let mut columns = first.output_column_info();
                columns.extend(second.output_column_info());
                columns
            }
        }
    }
}

impl TableContext for FromJoinClauseModel {
    fn contains_alias(&self, alias: &str) -> bool {
        match self {
            Self::FromClause {
                from,
                table_context,
            } => from.contains_alias(alias) || table_context.contains_alias(alias),
            Self::JoinClause { first, second, .. } => {
                first.contains_alias(alias) || second.contains_alias(alias)
            }
        }
    }
}

fn my_join_clause_strategy(
    (first, max_subquery_depth): (Rc<FromJoinClauseModel>, u8),
) -> impl Strategy<Value = (Rc<FromJoinClauseModel>, u8)> {
    let second_from_strategy = my_from_clause_leaf_strategy(first.clone(), max_subquery_depth);
    (Just(first), second_from_strategy).prop_map(move |(first, second)| {
        (
            Rc::new(FromJoinClauseModel::JoinClause {
                first,
                second,
                join_condition: JoinConditionModel::Cross,
            }),
            max_subquery_depth,
        )
    })
}

fn my_from_clause_strategy(
    table_context: Rc<dyn TableContext>,
    max_subquery_depth: u8,
) -> impl Strategy<Value = Rc<FromJoinClauseModel>> {
    let first_from_strategy =
        my_from_clause_leaf_strategy(table_context.clone(), max_subquery_depth);
    let leaf = first_from_strategy.prop_map(move |table| {
        (
            Rc::new(FromJoinClauseModel::FromClause {
                table_context: table_context.clone(),
                from: table,
            }),
            max_subquery_depth,
        )
    });
    leaf.prop_recursive(3, 2, 1, move |first_strategy| {
        first_strategy.prop_flat_map(my_join_clause_strategy)
    })
    .prop_map(|(join, _)| join)
}

#[derive(Debug, Clone)]
enum LiteralValueModel {
    Boolean,
    Integer,
    Real,
    Text,
    Blob,
}
impl LiteralValueModel {
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

#[derive(Debug, Clone)]
struct ColumnProjectionModel {
    source_alias: String,
    column: ColumnModel,
}
impl ColumnProjectionModel {
    fn as_sql_code(&self) -> String {
        format!("\"{}\".{}", self.source_alias, self.column.as_sql_code())
    }

    fn output_column_info(&self) -> ColumnInfo {
        self.column.to_column_info()
    }
}

impl std::convert::TryFrom<&OutputColumnInfo> for ColumnProjectionModel {
    type Error = ();
    fn try_from(value: &OutputColumnInfo) -> Result<Self, ()> {
        if let Some(name) = value.column_info.name {
            Ok(Self {
                source_alias: value.output_alias.clone(),
                column: ColumnModel {
                    name,
                    col_type: value.column_info.col_type,
                    nullable: value.column_info.nullable,
                },
            })
        } else {
            Err(())
        }
    }
}

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone)]
struct InfixNumericOperationModel {
    operation: InfixNumericOperationType,
    left: ExpressionModel,
    right: ExpressionModel,
}

impl InfixNumericOperationModel {
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

#[derive(Debug, Clone)]
enum ExpressionModel {
    LiteralValue(LiteralValueModel),
    ColumnProjection(ColumnProjectionModel),
    InfixNumericOperation(Box<InfixNumericOperationModel>),
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
        Just(ExpressionModel::LiteralValue(LiteralValueModel::Boolean)),
        Just(ExpressionModel::LiteralValue(LiteralValueModel::Integer)),
        Just(ExpressionModel::LiteralValue(LiteralValueModel::Real)),
        Just(ExpressionModel::LiteralValue(LiteralValueModel::Text)),
        Just(ExpressionModel::LiteralValue(LiteralValueModel::Blob)),
    ]
}

fn my_option_table_projection_expression_model_strategy(
    group_by_clause: Option<&GroupByModel>,
    from_clause: Option<&Rc<FromJoinClauseModel>>,
) -> Option<impl Strategy<Value = ExpressionModel>> {
    match (group_by_clause, from_clause) {
        (Some(groupby_clause), _) => {
            my_groupby_projection_expression_model_strategy(&groupby_clause).map(Strategy::boxed)
        }
        (None, Some(from_clause)) => {
            my_table_projection_expression_model_strategy(&from_clause).map(Strategy::boxed)
        }
        (None, None) => None,
    }
}

fn my_table_projection_expression_model_strategy(
    from_clause: &FromJoinClauseModel,
) -> Option<impl Strategy<Value = ExpressionModel>> {
    let column_models: Vec<ColumnProjectionModel> = from_clause
        .output_column_info()
        .iter()
        .filter_map(|c| c.try_into().ok())
        .collect();

    if column_models.is_empty() {
        None
    } else {
        Some(proptest::sample::select(column_models).prop_map(ExpressionModel::ColumnProjection))
    }
}

fn my_groupby_projection_expression_model_strategy(
    groupby_clause: &GroupByModel,
) -> Option<impl Strategy<Value = ExpressionModel>> {
    if groupby_clause.key_expressions.is_empty() {
        None
    } else {
        Some(proptest::sample::select(
            groupby_clause.key_expressions.clone(),
        ))
    }
}

fn my_expression_tree_strategy<E: 'static + Strategy<Value = ExpressionModel>>(
    leaf_strategy: E,
) -> impl Strategy<Value = ExpressionModel> + Clone {
    leaf_strategy.prop_recursive(1, 15, 2, move |leaf| {
        let operation_strategy = std::sync::Arc::new(prop_oneof![
            Just(InfixNumericOperationType::Add),
            Just(InfixNumericOperationType::Sub),
            Just(InfixNumericOperationType::Mul),
            //Just(InfixNumericOperationType::Div) //TODO: dividing by strings acts weird, sqlx doesn't implement that. ex: SELECT 1/"a"; -> null
        ]);
        (operation_strategy, leaf.clone(), leaf).prop_map(move |(operation, left, right)| {
            ExpressionModel::InfixNumericOperation(Box::new(InfixNumericOperationModel {
                operation,
                left,
                right,
            }))
        })
    })
}

#[derive(Debug, Clone)]
struct GroupByModel {
    pub key_expressions: Vec<ExpressionModel>,
    //pub value_expressions: Vec<Rc<ExpressionModel>>,
}

impl GroupByModel {
    pub fn as_sql_code(&self) -> String {
        if !self.key_expressions.is_empty() {
            format!(
                "GROUP BY {}",
                self.key_expressions
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

fn my_groupby_model_strategy<E: 'static + Strategy<Value = ExpressionModel>>(
    expression_strategy: E,
) -> impl Strategy<Value = GroupByModel> {
    let all_expressions_strategy = prop::collection::vec(expression_strategy, 0..3);
    all_expressions_strategy.prop_map(|expressions| GroupByModel {
        key_expressions: expressions,
    })
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
    let all_expressions_strategy = prop::collection::vec(orderby_expression_model_strategy, 0..3);
    all_expressions_strategy.prop_map(|expressions| OrderByModel { expressions })
}

#[derive(Debug, Clone, Copy)]
struct LimitOffsetModel {
    pub limit: isize,
    pub offset: Option<isize>,
}

impl LimitOffsetModel {
    pub fn as_sql_code(&self) -> String {
        if let Some(offset) = self.offset {
            format!("LIMIT {} OFFSET {}", self.limit, offset)
        } else {
            format!("LIMIT {}", self.limit)
        }
    }
}

fn my_limit_offset_strategy() -> impl Strategy<Value = LimitOffsetModel> {
    //avoiding limit 0, because if we return 0 results then any typing is 'safe'
    let limit_strategy = proptest::sample::select(vec![-1, 1, 1000000]);
    let offset_strategy =
        proptest::sample::select(vec![None, Some(-1), Some(0), Some(1), Some(1000000)]);

    (limit_strategy, offset_strategy).prop_map(|(limit, offset)| LimitOffsetModel { limit, offset })
}

#[derive(Debug, Clone)]
struct QueryModel {
    pub projections: Vec<ExpressionModel>,
    pub from: Option<Rc<FromJoinClauseModel>>,
    pub groupby: Option<GroupByModel>,
    pub orderby: OrderByModel,
    pub limit_offset: Option<LimitOffsetModel>,
}

impl QueryModel {
    pub fn as_sql_code(&self) -> String {
        format!(
            "SELECT {} {} {} {} {}",
            self.projections
                .iter()
                .map(|c| c.as_sql_code())
                .collect::<Vec<_>>()
                .join(","),
            match &self.from {
                Some(table) => table.as_sql_code(),
                None => String::new(),
            },
            match &self.groupby {
                Some(groupby) => groupby.as_sql_code(),
                None => String::new(),
            },
            self.orderby.as_sql_code(),
            match &self.limit_offset {
                Some(limit_offset) => limit_offset.as_sql_code(),
                None => String::new(),
            },
        )
    }

    pub fn as_sql_code_distinct_types(&self) -> String {
        format!("SELECT DISTINCT * FROM ({}) LIMIT 100", self.as_sql_code())
    }

    fn output_column_info(&self) -> Vec<ColumnInfo> {
        self.projections
            .iter()
            .map(|c| c.output_column_info())
            .collect()
    }
}

fn my_query_strategy(
    table_context: Rc<dyn TableContext>,
    max_subquery_depth: u8,
) -> impl Strategy<Value = QueryModel> {
    let from_clause_strategy = proptest::option::of(my_from_clause_strategy(
        table_context.clone(),
        max_subquery_depth - 1,
    ));

    from_clause_strategy.prop_flat_map(move |from_clause| {
        let table_column_projection_strategy =
            my_option_table_projection_expression_model_strategy(None, from_clause.as_ref());

        let expression_leaf_strategy =
            if let Some(table_column_projection_strategy) = table_column_projection_strategy {
                prop_oneof![
                    my_literal_expression_model_strategy(),
                    table_column_projection_strategy
                ]
                .boxed()
            } else {
                my_literal_expression_model_strategy().boxed()
            };

        let expression_model_strategy = expression_leaf_strategy; //my_expression_tree_strategy(expression_leaf_strategy);

        let groupby_strategy =
            proptest::option::of(my_groupby_model_strategy(expression_model_strategy));

        (Just(from_clause), groupby_strategy).prop_flat_map(move |(from_clause, groupby_clause)| {
            let column_projection_strategy = my_option_table_projection_expression_model_strategy(
                groupby_clause.as_ref(),
                from_clause.as_ref(),
            );

            let expression_leaf_strategy =
                if let Some(column_projection_strategy) = column_projection_strategy {
                    prop_oneof![
                        my_literal_expression_model_strategy(),
                        column_projection_strategy
                    ]
                    .boxed()
                } else {
                    my_literal_expression_model_strategy().boxed()
                };

            let expression_model_strategy = expression_leaf_strategy; //my_expression_tree_strategy(expression_leaf_strategy);

            let select_columns_strategy =
                prop::collection::vec(expression_model_strategy.clone(), 1..3);
            let orderby_strategy = my_orderby_strategy(expression_model_strategy);
            let limit_offset_strategy = proptest::option::of(my_limit_offset_strategy());

            (
                select_columns_strategy, //todo: use group by outputs instead of table outputs
                Just(from_clause),
                Just(groupby_clause),
                orderby_strategy,
                limit_offset_strategy,
            )
                .prop_map(
                    move |(projections, from, groupby, orderby, limit_offset)| QueryModel {
                        projections,
                        from,
                        groupby,
                        orderby,
                        limit_offset,
                    },
                )
        })
    })
}

proptest! {
    #[test]
    fn describe_query_prop_test(query_model in my_query_strategy(Rc::new(()),3)) {
        eprintln!("QUERY MODEL:{:?}\nQUERY:{}\nEXPECTED:{:?}",query_model, query_model.as_sql_code(),query_model.output_column_info());
        let res = ::sqlx_rt::async_std::task::block_on(async{
            let mut conn = new::<Sqlite>().await.unwrap();
            {
                let info = conn.describe(&query_model.as_sql_code()).await.unwrap();

                let columns = info.columns();

                for (i,expected_column) in query_model.output_column_info().iter().enumerate()
                {
                    if let Some(expected_column_name) = expected_column.name
                    {
                        prop_assert_eq!(&columns[i].name(), &expected_column_name);
                    }
                    prop_assert_eq!(info.nullable(i), Some(expected_column.nullable), "column {}", i);
                    prop_assert_eq!(columns[i].type_info().name(), expected_column.col_type.name(), "column {}", i);
                }
            }

            let rows = conn.fetch_all(
                &*query_model.as_sql_code_distinct_types()).await.unwrap();

            for row in rows
            {
                for (i,expected_column) in query_model.output_column_info().iter().enumerate()
                {
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
