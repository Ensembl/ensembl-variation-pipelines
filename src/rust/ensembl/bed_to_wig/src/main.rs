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
 
use std::{io::{BufReader, Write, BufRead}, env, fs::File};

fn main() {
    let args = env::args().collect::<Vec<_>>();
    let buf = BufReader::new(File::open(&args[1]).unwrap());
    let mut out = File::create(&args[2]).unwrap();
    
    let mut cur_chr = String::new();
    let mut pos: u64 = 1;
    for line in buf.lines() {
        let parts = line.unwrap().split(" ").map(|s| s.to_string()).collect::<Vec<_>>();
        
        // bed is 0-indexed and wiggle is 1-indexed, so we need to add 1 here
        let start = parts[1].parse::<u64>().unwrap() + 1;
        // bed is end-inclusive and wiggle is end-inclusive, so no need to add 1 here
        let mut end = parts[2].parse::<u64>().unwrap();
        // for insertion we will have start > end after above transformation - account for that
        if start > end {
            end += 1;
        }
        
        
        if cur_chr.is_empty() || cur_chr != parts[0] {
            write!(out, "fixedStep  chrom={} start=1 step=1\n", parts[0]).unwrap();
            
            pos = 1;
            cur_chr = parts[0].clone();
        }
        
        while pos <= end {
            if pos < start {
                write!(out, "0\n").unwrap();
            }
            else {
                write!(out, "{}\n", parts[7]).unwrap();
            }
            
            pos += 1;
        }
    }
}