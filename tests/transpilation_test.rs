use titan_engine::fingerprint::normalize::Normalizer;
use polyglot_sql::{parse_one, generate, DialectType};

#[test]
fn test_complex_transpilation_across_dialects() {
    let pg_sql = "
        SELECT 
            id,
            name,
            JSON_EXTRACT_PATH_TEXT(data, 'email') as email,
            RANK() OVER (PARTITION BY org_id ORDER BY created_at DESC) as rnk
        FROM users
        WHERE created_at > '2023-01-01'
        LIMIT 100
    ";

    // 1. Parse as Postgres (our internal Lingua Franca)
    let ast = parse_one(pg_sql, DialectType::PostgreSQL).unwrap();

    // 2. Transpile to MySQL
    let mysql_sql = generate(&ast, DialectType::MySQL).unwrap();
    println!("MySQL Transpilation:\n{}", mysql_sql);
    assert!(mysql_sql.contains("LIMIT 100"));

    // 3. Transpile to Snowflake
    let snowflake_sql = generate(&ast, DialectType::Snowflake).unwrap();
    println!("Snowflake Transpilation:\n{}", snowflake_sql);
    assert!(snowflake_sql.contains("LIMIT 100"));

    // 4. Verify they all result in the same fingerprint when normalized back to Postgres
    let norm_mysql = Normalizer::normalize(&mysql_sql).unwrap();
    let norm_snowflake = Normalizer::normalize(&snowflake_sql).unwrap();
    
    assert_eq!(norm_mysql.as_str(), norm_snowflake.as_str(), "Dialect-specific transpilation should be reversible to a canonical form");
}
