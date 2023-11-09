use std::fs::File;
use std::io::Write;
use actix_web::{post, App, web, HttpResponse, HttpServer, Responder};
use mysql_async::{prelude::Queryable, Opts};
use dotenv::dotenv;
use serde_json::{Map, Value as JsonValue};
use env_logger;
use log;
use csv;
use serde_json::Value;
use csv::Writer;

// create async function to handle exec mysql
#[post("/exec")]
async fn exec(pool_exec: web::Data<PoolExec>,s_sql: String) -> impl Responder {
    // get ip address of client
    // let ip = req.peer_addr().map(|addr| addr.ip().to_string()).unwrap_or_default();
    let pool = &pool_exec.0;

    let mut conn = pool.get_conn().await.unwrap();
    conn.exec_drop(s_sql, ()).await.unwrap();
    // drop(conn);
    HttpResponse::Ok().body("DONE")
}

// create sync function to handle query mysql
async fn query_db(pool: mysql_async::Pool, s_sql:String)-> Vec<JsonValue>{
    let mut conn = pool.get_conn().await.unwrap();
    let mut json_arr = Vec::new();

    let mut resultset = conn.exec_iter(s_sql, ()).await.unwrap();

    let columns = resultset.columns_ref();

    // create list string to store column name
    let mut column_names = Vec::new();

    for column in columns {
        let column_name = format!("{}", column.name_str());
        column_names.push(column_name);
    }

    _ = resultset.for_each(|row| {
        let row = row.unwrap();

        let mut json_obj = Map::new();
        let num_cols = row.len();

        for i in 0..num_cols {
            // get column name
            let col_name = format!("{}", column_names[i]);
            // get column value
            let col_value = JsonValue::String(format!("{:?}",row.get(i).unwrap().as_sql(true).as_str()).replace("\"", "").replace("'", ""));

            json_obj.insert(col_name, col_value);
        }

        json_arr.push(JsonValue::Object(json_obj));
    }).await;
    drop(conn);
    json_arr
}

// create async function to handle query mysql
#[post("/query")]
async fn query(pool_query: web::Data<PoolQuery>,s_sql: String) -> impl Responder {
    let pool = &pool_query.0;
    let json_arr = query_db(pool.clone(),s_sql).await;
    HttpResponse::Ok().body(serde_json::to_string(&json_arr).unwrap())
}

#[post("/query_to_file/{type_file}/{name_file}")]
async fn query_to_file(pool_query: web::Data<PoolQuery>,s_sql: String, path: web::Path<(String, String)>) -> impl Responder {
    let (type_file, name_file) = path.into_inner();
    let pool = &pool_query.0;
    let json_arr = query_db(pool.clone(),s_sql).await;

    if type_file == "json" {
        let mut file = File::create(format!("DATA/{}.{}", name_file, type_file)).unwrap();
        // save json_arr to file
        file.write_all(serde_json::to_string(&json_arr).unwrap().as_bytes()).unwrap();
        // close file
        drop(file);
    }
    else if type_file == "csv" {
        let mut csv_writer = Writer::from_path(format!("DATA/{}.{}", name_file, type_file)).unwrap();

        if let Some(first_item) = json_arr.first() {
            if let Value::Object(obj) = first_item {
                let csv_headers: Vec<String> = obj.keys().map(|key| key.to_string()).collect();
                csv_writer.write_record(&csv_headers).unwrap();

                for item in &json_arr {
                    if let Value::Object(obj) = item {
                        let csv_row: Vec<String> = csv_headers
                            .iter()
                            .map(|key| {
                                obj.get(key)
                                    .and_then(|v| v.as_str())
                                    .unwrap_or_default()
                                    .to_string()
                            })
                            .collect();

                        csv_writer.write_record(&csv_row).unwrap();
                    }
                }
            }
        }

        csv_writer.flush().expect("TODO: panic message");
    }
    else {
        return HttpResponse::Ok().body("Type file not support");
    }

    HttpResponse::Ok().body("DONE")
}


struct PoolExec(mysql_async::Pool);
struct PoolQuery(mysql_async::Pool);

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::builder().filter_level(log::LevelFilter::Error).init();
    dotenv().ok();

    let db_exec_user = std::env::var("EXEC_DB_USER").expect("EXEC_DB_USER must be set.");
    let db_exec_pass = std::env::var("EXEC_DB_PASS").expect("EXEC_DB_PASS must be set.");
    let db_exec_host = std::env::var("EXEC_DB_HOST").expect("EXEC_DB_HOST must be set.");
    let db_exec_name = std::env::var("EXEC_DB_NAME").expect("EXEC_DB_NAME must be set.");
    let db_exec_port = std::env::var("EXEC_DB_PORT").expect("EXEC_DB_PORT must be set.");

    let db_query_user = std::env::var("QUERY_DB_USER").expect("QUERY_DB_USER must be set.");
    let db_query_pass = std::env::var("QUERY_DB_PASS").expect("QUERY_DB_PASS must be set.");
    let db_query_host = std::env::var("QUERY_DB_HOST").expect("QUERY_DB_HOST must be set.");
    let db_query_name = std::env::var("QUERY_DB_NAME").expect("QUERY_DB_NAME must be set.");
    let db_query_port = std::env::var("QUERY_DB_PORT").expect("QUERY_DB_PORT must be set.");


    println!("DB_EXEC_HOST: {} - DB_EXEC_PORT: {}",db_exec_host, db_exec_port);
    println!("DB_QUERY_HOST: {} - DB_QUERY_PORT: {}",db_query_host, db_query_port);

    let url_exec:Opts = Opts::from_url(&format!("mysql://{}:{}@{}:{}/{}",db_exec_user,db_exec_pass,db_exec_host,db_exec_port,db_exec_name)).unwrap();
    let url_query:Opts = Opts::from_url(&format!("mysql://{}:{}@{}:{}/{}",db_query_user,db_query_pass,db_query_host,db_query_port,db_query_name)).unwrap();

    let pool_exec = mysql_async::Pool::new(url_exec);
    let pool_query = mysql_async::Pool::new(url_query);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(PoolExec(pool_exec.clone())))
            .app_data(web::Data::new(PoolQuery(pool_query.clone())))
            .service(exec)
            .service(query)
            .service(query_to_file)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
