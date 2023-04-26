use std::{io::{BufReader,Write}, fs::File, env, collections::HashMap, collections::HashSet};
use vcf::{VCFError, VCFReader};
use flate2::read::MultiGzDecoder;

const VARIANTGROUP : [(&str, u8); 45] = [
    ("frameshift_variant", 1),
    ("inframe_deletion", 1),
    ("inframe_insertion", 1),
    ("missense_variant", 1),
    ("protein_altering_variant", 1),
    ("start_lost", 1),
    ("stop_gained", 1),
    ("stop_lost", 1),
    ("splice_acceptor_variant", 2),
    ("splice_donor_5th_base_variant", 2),
    ("splice_donor_region_variant", 2),
    ("splice_donor_variant", 2),
    ("splice_polypyrimidine_tract_variant", 2),
    ("splice_region_variant", 2),
    ("3_prime_UTR_variant", 3),
    ("5_prime_UTR_variant", 3),
    ("coding_sequence_variant", 3),
    ("incomplete_terminal_codon_variant", 3),
    ("intron_variant", 3),
    ("mature_miRNA_variant", 3),
    ("NMD_transcript_variant", 3),
    ("non_coding_transcript_exon_variant", 3),
    ("non_coding_transcript_variant", 3),
    ("start_retained_variant", 3),
    ("stop_retained_variant", 3),
    ("synonymous_variant", 3),
    ("feature_elongation", 3),
    ("feature_truncation", 3),
    ("transcript_ablation", 3),
    ("transcript_amplification", 3),
    ("transcript_fusion", 3),
    ("transcript_translocation", 3),
    ("regulatory_region_variant", 4),
    ("TF_binding_site_variant", 4),
    ("regulatory_region_ablation", 4),
    ("regulatory_region_amplification", 4),
    ("regulatory_region_fusion", 4),
    ("regulatory_region_translocation", 4),
    ("TFBS_ablation", 4),
    ("TFBS_amplification", 4),
    ("TFBS_fusion", 4),
    ("TFBS_translocation", 4),
    ("upstream_gene_variant", 5),
    ("downstream_gene_variant", 5),
    ("intergenic_variant", 5)
];

struct Line {
    chromosome: String,
    start: u64,
    end: u64,
    id: String,
    variety: String,
    reference: String,
    alts: HashSet<String>,
    group: u8,
    severity: String,
    severity_rank: u8
}

impl Line {
    fn compatible(&self, other: &Line) -> bool {
        self.chromosome == other.chromosome &&
        self.start == other.start &&
        self.variety == other.variety &&
        self.reference == other.reference
    }
    
    fn redundant(&self, other: &Line) -> bool {
        self.id == other.id && 
        self.variety != other.variety 
    }
    
    fn merge(&mut self, mut more: Option<Line>, out: &mut File) {
        // merge new line if not empty (and a Line instance)
        if let Some(ref mut more) = more {
            if self.compatible(more) {
                self.alts.extend(more.alts.clone());
                if more.severity_rank < self.severity_rank {
                    if more.end > self.end {
                        self.end = more.end;
                        self.variety = more.variety.clone();
                    }
                    self.group = more.group;
                    self.severity = more.severity.to_string();
                    self.severity_rank = more.severity_rank;
                }
                return;
            }
            
            // if somehow with same rs id we have different variety of variant we skip the later ones
            if self.redundant(more) {
                return
            }
        }
        
        // if new Line is not compatible with the current one it is a new variant
        // print out the current line
        if self.alts.len() > 0 {
            let alts = Vec::from_iter(self.alts.clone());
            write!(out, "{} {} {} {} {} {} {} {} {}\n",
                self.chromosome, self.start, self.end,
                self.id, self.variety, self.reference,
                alts.join("/"), self.group, self.severity
            ).unwrap();
        }
        
        // make the new Line as the current one
        if let Some(more) = more {
            *self = more;
        }
    }
}

fn main() -> Result<(), VCFError> {
    // read cli arguments
    let args = env::args().collect::<Vec<_>>();
    let mut reader = VCFReader::new(BufReader::new(MultiGzDecoder::new(File::open(
        &args[1]
    )?)))?;
    let mut out = File::create(&args[2]).unwrap();
    let json = std::fs::read_to_string(&args[3]).unwrap();
        
    let severity = {
        serde_json::from_str::<HashMap<String, String>>(&json).unwrap()
    };
    
    // create the severity hash
    let mut variant_groups = HashMap::new();
    for (csq, value) in &VARIANTGROUP {
        variant_groups.insert(csq.to_string(), *value);
    }
    
    let mut record = reader.empty_record();
    // dummy initial value for the object to read line from vcf
    // this line is guranteed to not get printed as alt.len == 0
    let mut lines = Line {
        chromosome: "".to_string(),
        start: 1,
        end: 0,
        id: "".to_string(),
        variety: "".to_string(),
        reference: "".to_string(),
        alts: HashSet::new(),
        group: 0,
        severity: "".to_string(),
        severity_rank: 255
    };
    while reader.next_record(&mut record)? {
        let reference = String::from_utf8(record.reference.clone()).unwrap();
        let ref_len = reference.len() as u64;
        // for now - we ignore ref with more than 31 char name becaus of bedToBigBed failure
        if ref_len > 31 { continue; }
        
        let mut multiple_ids = false;
        let ids = record.id.iter().map(|b| {
            String::from_utf8(b.clone())
        }).collect::<Result<Vec<_>,_>>().unwrap();
        // for now - we assume a variant cannot have mutliple ids
        for id in ids.iter() {
            if id.contains(";") { multiple_ids = true; }
        }
        if multiple_ids { continue; }
        
        let alts = record.alternative.iter().map(|a| {
            String::from_utf8(a.clone())
        }).collect::<Result<HashSet<_>,_>>().unwrap();
        
        let csq = record.info(b"CSQ").map(|csqs| {
            csqs.iter().map(|csq| {
                let s = String::from_utf8_lossy(csq);
                s.split("|").nth(1).unwrap_or("").to_string()
            }).collect::<Vec<String>>()
        }).unwrap_or(vec![]);
        // if csq is empty we won't have most severe consequence
        if csq.is_empty(){ continue; }
        
        let class = record.info(b"CSQ").map(|csqs| {
            csqs.iter().map(|csq| {
                let s = String::from_utf8_lossy(csq);
                s.split("|").nth(21).unwrap_or("").to_string()
            }).collect::<Vec<String>>()
        }).unwrap_or(vec![]);
        
        for id in ids.iter() {
            for alt in alts.iter() {
                let mut variant_group = 0;
                let mut most_severe_csq = "";
                let mut msc_rank = 255;
                let mut variety = "";
                
                for (csq, variety_here) in csq.iter().zip(class.iter()) {
                    for csq in csq.split("&") {
                        let severity_here = (*severity.get(csq).unwrap_or(&String::from("0"))).parse::<u8>().unwrap();
                        if severity_here < msc_rank {
                            variant_group = *variant_groups.get(csq).unwrap_or(&0);
                            most_severe_csq = csq;
                            msc_rank = severity_here;
                            
                            // variety should always be same for each variant 
                            // dbSNP merges all variants that have variety in SPDI notation but in vcf we can see different variety for same variant
                            // VEP though would report "sequence_alteration" for all if there are different variety in a variant
                            variety = variety_here;
                        }
                    }
                }
                
                // what happens when the variety is "sequence_alteration"
                // we will take end = start + ref length - 1, which is true for all except insertion and SNV
                // for insertion it is alright, because the other variety will always have larger end 
                // for SNV it is also alright, because it should not be appearing in a "sequence_alteration" in the first place
                let mut end = record.position + ref_len - 1;
                if variety.eq(&String::from("SNV")) {
                    end = record.position;
                }
                else if variety.eq(&String::from("insertion")) {
                    end = record.position + 1;
                }
                
                let more = Line {
                    chromosome: String::from_utf8(record.chromosome.to_vec()).unwrap(),
                    start: record.position,
                    end: end,
                    id: id.to_string(),
                    variety: variety.to_string(),
                    reference: reference.clone(),
                    alts: HashSet::from([alt.to_string()]),
                    group: variant_group,
                    severity: most_severe_csq.to_string(),
                    severity_rank: msc_rank
                };
                
                lines.merge(Some(more), &mut out);
            }
        }
    }
    
    lines.merge(None, &mut out);
    Ok(())
}