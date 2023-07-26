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
        self.id == other.id &&
        self.start == other.start &&
        self.reference == other.reference &&
        self.variety == other.variety
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
                alts.join(","), self.group, self.severity
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
            let mut variant_group = 0;
            let mut most_severe_csq = "";
            let mut most_severe_csq_rank = 255;
            
            // calculate most severe consequence and variant group of that consequence
            for csq_str in csq.iter() {
                for csq_here in csq_str.split("&") {
                    let csq_rank_here = (*severity.get(csq_here).unwrap_or(&String::from("0"))).parse::<u8>().unwrap();
                    if csq_rank_here < most_severe_csq_rank {
                        variant_group = *variant_groups.get(csq_here).unwrap_or(&0);
                        most_severe_csq = csq_here;
                        most_severe_csq_rank = csq_rank_here;
                    }
                }
            }
            
            // calcualte variant class - we store it as variety
            // variety should always be same for each variant allele - VEP puts variant class at variant level (using Bio::EnsEMBL::Variation::Utils::Sequence::SO_variation_class)
            // if cannot be deduced the default value is - sequence_alteration
            let mut variety = class[0].to_string();
            
            // if sequence_alteration we check if we can convert it to indel (the condition is that all the variant allele is eiter insertion or deletion or indel)
            if variety.eq(&String::from("sequence_alteration")) {
                let mut convert_sequence_alteration = true;
                for alt in alts.iter() {
                    // note that we are not minimilizing the variant alleles here 
                    let calc_variety = match (alt.len()<2, reference.len()<2, alt.len() == reference.len()) {
                        (true, true, true) => { "SNV" },
                        (true, false, false) => { "deletion" },
                        (false, true, false) => { "insertion" },
                        (false, false, false) => { "indel" },
                        (false, false, true) => { "substitute" },
                        _ => todo!(),
                    };
                    
                    // if any of the variant allele is SNV or substitution we will log (because this is not a regualr case)
                    // and, keep the variety as sequence_alteration
                    if calc_variety.eq(&String::from("SNV")) || calc_variety.eq(&String::from("substitute")) {
                        println!("[WARNING] sequence_alteration variant ({0} {1}:{2}) contain variant allele of type {3}",
                            id, 
                            String::from_utf8(record.chromosome.to_vec()).unwrap(), 
                            record.position,
                            calc_variety
                        );
                
                        convert_sequence_alteration = false;
                        break;
                    }
                }
                
                if convert_sequence_alteration {
                    variety = "indel".to_string();
                }
            }

            // start position in bed is 0-indexed
            let mut start = record.position - 1;
        
            // end in bed is exclusive
            let mut end = record.position + ref_len;
            if variety.eq(&String::from("insertion")) {
                end = start;
            }
            
            let more = Line {
                chromosome: String::from_utf8(record.chromosome.to_vec()).unwrap(),
                start: start,
                end: end,
                id: id.to_string(),
                variety: variety,
                reference: reference.clone(),
                alts: alts.clone(),
                group: variant_group,
                severity: most_severe_csq.to_string(),
                severity_rank: most_severe_csq_rank
            };
            
            lines.merge(Some(more), &mut out);
        }
    }
    
    lines.merge(None, &mut out);
    Ok(())
}