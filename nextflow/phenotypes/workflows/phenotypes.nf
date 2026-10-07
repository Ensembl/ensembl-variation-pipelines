include { CLINVAR } from './clinvar'

workflow PHENOTYPES {
    main:
    CLINVAR()

    emit:
    records = CLINVAR.out.records
    summary = CLINVAR.out.summary
    warnings = CLINVAR.out.warnings
    rejections = CLINVAR.out.rejections
}
