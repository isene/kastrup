// Bind any reachable F-key in this terminal. Prints the key string
// crust's Input::getchr resolves it to. Ctrl-C to exit.
use crust::{Crust, Input};

fn main() {
    Crust::init();
    println!("Press F-keys (Q to quit). Press any other key to see its tag:\r");
    loop {
        if let Some(k) = Input::getchr(None) {
            println!("→ {:?}\r", k);
            if k == "Q" || k == "q" { break; }
        }
    }
    Crust::cleanup();
}
