use std::fs;
use std::env;
use std::fs::File;
use std::io::{Write, Read};

pub fn list_directories(relative_path: &Vec<&str>) -> Vec<String> {
    let mut dir = env::current_dir().unwrap();
    for path in relative_path {
        dir.push(path);
    }
    let mut directories = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            let pathend = path.file_name().unwrap();
            directories.push(pathend.to_str().unwrap().to_string());
        }
    }
    directories
}

pub fn load_program(relative_path: &Vec<&str>, program: &str) -> Vec<u8> {
    let mut dir = env::current_dir().unwrap();
    for path in relative_path {
        dir.push(path);
    }
    dir.push(program);
    dir.push(program.replace('-', "_") + "_program.so");
    let mut file = File::open(dir.clone()).expect("file not found");
    let metadata = fs::metadata(dir).expect("Unable to read metadata");
    let mut buffer = vec![0; metadata.len() as usize];
    file.read_exact(&mut buffer).expect("Buffer overflow");
    buffer
}

fn save_to_disk(filename: &str, data: &[u8]) -> std::io::Result<()> {
    let mut file = File::create(filename)?;
    file.write_all(data)?;
    Ok(())
}

fn create_load_function(hardcoded_file_bytes: &mut String, loadable_programs: &Vec<String>) {
    hardcoded_file_bytes.push_str("#[allow(dead_code)]\n");
    hardcoded_file_bytes.push_str("pub fn load_program(program_name: String) -> Vec<u8> {\n");
    hardcoded_file_bytes.push_str("    match program_name.as_str() {\n");
    for program in loadable_programs {
        hardcoded_file_bytes.push_str(&format!("        \"{}\" => Vec::from({}),\n", program, program.replace("-", "_").to_uppercase()));
    }
    hardcoded_file_bytes.push_str("        _ => panic!(\"unknown program\"),\n");
    hardcoded_file_bytes.push_str("    }\n");
    hardcoded_file_bytes.push_str("}\n\n");
}

pub fn main() {
    let relative_path = ["..", "..", "agave", "svm", "tests", "example-programs"].to_vec();
    let loadable_programs = list_directories(&relative_path);
    println!("Loadable programs: {:?}", loadable_programs);
    let mut hardcoded_file_bytes: String = String::with_capacity(100_000);
    hardcoded_file_bytes.push_str("/// Automatically generated file, please do not edit manually!\n");
    hardcoded_file_bytes.push_str(format!("/// Loadable programs: {:?}\n\n", loadable_programs).as_str());

    create_load_function(&mut hardcoded_file_bytes, &loadable_programs);

    for program in loadable_programs {
        let program_bytes = load_program(&relative_path, &program);
        
        hardcoded_file_bytes.push_str("#[allow(dead_code)]\n");
        hardcoded_file_bytes.push_str(&format!("const {}: [u8; {}] = {:?};\n\n", program.replace("-", "_").to_uppercase(), program_bytes.len(), program_bytes));
    }

    save_to_disk("hardcoded_programs.rs", hardcoded_file_bytes.as_bytes()).unwrap();
}