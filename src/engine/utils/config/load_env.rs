use dotenvy::dotenv;
use std::env;

pub fn load_env() -> Vec<String> {
    dotenv().ok();
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");
    let table_name = env::var("DATASET_TABLE").unwrap_or_else(|_| "no_key_found".to_string());

    println!("Подключаюсь к базе: {}", db_url);
    println!("Имя таблицы: {}", table_name);
    vec![db_url, table_name]
}
