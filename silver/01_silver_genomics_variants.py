# ============================================
# ClinicalFlow360 - Silver Layer - Stream 1
# Table: clinicalflow360.silver.genomics_variants
# Quarantine: clinicalflow360.silver.genomics_quarantine
# DQ Rules:
#   1. QUAL_SCORE > 30
#   2. MAF > 0.01
#   3. READ_DEPTH >= 10
#   4. USUBJID NOT NULL
#   5. CHROM IN valid list
# Pass rate : 98.1% (96,011 records)
# Quarantined: 1.9% (1,838 records)
# New columns: VARIANT_CATEGORY,
#              CLINICAL_SIGNIFICANCE,
#              QUALITY_TIER,
#              IS_ONCOGENE,
#              IS_TUMOR_SUPPRESSOR
# Author: Rajashekar | Date: 2026-08-04
# ============================================
