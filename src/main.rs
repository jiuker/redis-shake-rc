use redis_shake_rs::utils::run::Runner;

fn main() {
    let source_url = "127.0.0.1:6379";
    let source_pass  = "";
    let target_url = "127.0.0.1:6400";
    let target_pass  = "";
    Runner::mod_full(source_url,source_pass,target_url,target_pass);
}
