use std::env;

pub struct Env {
    pub database_url: String,
}

pub fn load_env() -> Env {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");
    // let table_name = env::var("DATASET_TABLE").unwrap_or_else(|_| "no_key_found".to_string());
    Env {
        database_url: db_url,
        // dataset_table: table_name
    }
}
