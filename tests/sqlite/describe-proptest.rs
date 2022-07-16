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
struct QueryModel {
    pub projections: Vec<ColumnModel>,
    pub table: &'static str,
}

fn my_query_strategy() -> impl Strategy<Value = QueryModel> {
    prop_oneof![
        prop::collection::vec(my_tweet_column_strategy(), 1..100).prop_map(|projections| {
            QueryModel {
                projections,
                table: "tweet",
            }
        }),
        prop::collection::vec(my_accounts_column_strategy(), 1..100).prop_map(|projections| {
            QueryModel {
                projections,
                table: "accounts",
            }
        }),
        prop::collection::vec(my_accounts_column_strategy(), 1..100).prop_map(|projections| {
            QueryModel {
                projections,
                table: "accounts_view",
            }
        }),
    ]
}

proptest! {
    #[test]
    fn describe_query_prop_test(query_model in my_query_strategy())  {
        eprintln!("{:?}",query_model);
        let res = ::sqlx_rt::async_std::task::block_on(async{
            let mut conn = new::<Sqlite>().await.unwrap();
            let info = conn.describe(
                &format!(
                "SELECT {} FROM {}",
                query_model.projections.iter().map(|c|c.name).collect::<Vec<_>>().join(","),
                query_model.table
            )).await.unwrap();

            let columns = info.columns();

            for (i,col_model) in query_model.projections.iter().enumerate()
            {
                prop_assert_eq!(columns[i].name(), col_model.name);
                prop_assert_eq!(info.nullable(i), Some(col_model.nullable));
                prop_assert_eq!(columns[i].type_info().name(), col_model.typename);
            }

            Ok(())
        });
        res.unwrap()
    }
}
