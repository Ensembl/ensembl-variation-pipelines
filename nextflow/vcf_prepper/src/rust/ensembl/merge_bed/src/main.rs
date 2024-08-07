/*
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
use std::{io::{BufReader, BufRead, Write}, fs::File, env, collections::{HashSet}};

fn main() {
    // read cli arguments
    let args = env::args().collect::<Vec<_>>();
    let mut out = File::create(&args[1]).unwrap();

    let mut current_ids = HashSet::new();
    let mut file_counter = 2;
    while file_counter < args.len() {
        let reader = BufReader::new(File::open(&args[file_counter]).unwrap());
        for line in reader.lines() {
            
            let parts = line.unwrap().split(" ").map(|s| s.to_string()).collect::<Vec<_>>();
            
            if !current_ids.contains(&parts[3]) {
                write!(out, "{} {} {} {} {} {} {} {} {}\n",
                    parts[0], 
                    parts[1],
                    parts[2],
                    parts[3], 
                    parts[4], 
                    parts[5],
                    parts[6],
                    parts[7],
                    parts[8]
                ).unwrap();
                
                current_ids.insert(parts[3].clone());
            }
        }
        
        file_counter += 1;
    }
}