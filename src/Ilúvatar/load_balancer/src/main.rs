use actix_web::{
  middleware,
  web::{self, Data},
  App, HttpRequest, HttpResponse, HttpServer, get
};

struct LBServer {
  
}

impl LBServer {
  pub fn index(&self) {
    println!("INDEX");
  }
  pub fn name(&self, name_str: &String) {
    println!("server: {}", name_str);
  }
}

#[get("/{name}")]
async fn name(app: Data<LBServer>, req: HttpRequest, name: web::Path<String>) -> HttpResponse {
  println!("{req:?}");
  app.name(&name);
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

async fn index(app: Data<LBServer>, req: HttpRequest) -> HttpResponse {
  println!("{req:?}");
  app.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
  let server = LBServer {};
  let server_data = Data::new(server);
  
  // move is necessary to give closure below ownership of counter1
  HttpServer::new(move || {
      App::new()
      .app_data(server_data.clone())
          .service(web::resource("/").to(index))
          .service(name)
  })
  .bind(("127.0.0.1", 8080))?
  .run()
  .await
}