
# ============================================
# ClinicalFlow360 - Bronze Layer - Stream 1
# CRO: Covance LabCorp | Format: MAF schema
# Table: clinicalflow360.bronze.genomics_raw
# Author: Rajashekar | Date: 2026-08-04
# ============================================

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def generate_genomics_data(patient_count=500):
    """
    Generate MAF-schema genomics data
    Same structure as Covance LabCorp delivery
    """
    random.seed(42)
    np.random.seed(42)

    GENES = [
        "TP53","KRAS","EGFR","STK11","KEAP1","NF1",
        "BRAF","MET","ARID1A","CDKN2A","RB1","PIK3CA",
        "BRCA1","BRCA2","ALK","ROS1","ERBB2"
    ]
    CHROMS    = [str(i) for i in range(1,23)] + ["X","Y"]
    VAR_TYPES = ["SNP","INS","DEL"]
    VAR_CLASS = ["Missense_Mutation","Nonsense_Mutation",
                 "Silent","Frame_Shift_Del","Splice_Site"]
    PLATFORMS = ["Illumina_NovaSeq","Illumina_HiSeq","MGI_DNBSEQ"]
    TUMORS    = ["LUAD","LUSC","BRCA","COAD","GBM"]

    records = []
    for i in range(1, patient_count + 1):
        pid   = f"TCGA-{random.choice(['55','38','44','64','78'])}-{i:04d}"
        tumor = random.choice(TUMORS)
        site  = f"SITE-{random.randint(1,45):03d}"
        arm   = random.choice(["ARM_A","ARM_B","ARM_C"])

        for _ in range(random.randint(100, 300)):
            ref  = random.choice(["A","T","C","G"])
            gene = random.choice(GENES)
            qual = round(np.random.beta(8,2)*100, 2)

            records.append({
                "USUBJID":         pid,
                "STUDYID":         "CLINFLOW-LUAD-2024",
                "SITE_ID":         site,
                "TREATMENT_ARM":   arm,
                "TUMOR_TYPE":      tumor,
                "CHROM":           random.choice(CHROMS),
                "POS":             random.randint(1000000,250000000),
                "REF_ALLELE":      ref,
                "ALT_ALLELE":      random.choice(
                                   [b for b in ["A","T","C","G"]
                                   if b != ref]),
                "GENE_NAME":       gene,
                "VARIANT_TYPE":    random.choice(VAR_TYPES),
                "VARIANT_CLASS":   random.choice(VAR_CLASS),
                "QUAL_SCORE":      qual,
                "READ_DEPTH":      random.randint(10, 500),
                "MAF":             round(random.uniform(0.001,0.5),4),
                "TUMOR_VAF":       round(random.uniform(0.05,0.95),4),
                "PATHOGENIC_FLAG": "Y" if gene in
                                   ["TP53","BRCA1","BRCA2","KRAS"]
                                   and qual > 70 else "N",
                "SEQ_DATE":        (datetime(2024,1,1) +
                                   timedelta(days=random.randint(0,365))
                                   ).strftime("%Y-%m-%d"),
                "PLATFORM":        random.choice(PLATFORMS),
                "CRO_SOURCE":      "Covance_LabCorp"
            })
    return pd.DataFrame(records)


def run_bronze_ingestion():
    """
    Main bronze ingestion function
    Reads raw data → saves to Delta bronze table
    """
    print("=" * 55)
    print("BRONZE INGESTION — Stream 1 Genomics")
    print("=" * 55)

    # Generate data
    df_pandas = generate_genomics_data(patient_count=500)
    print(f"Records generated : {len(df_pandas):,}")

    # Convert to Spark DataFrame
    df_spark = spark.createDataFrame(df_pandas)

    # Write to Unity Catalog bronze table
    df_spark.write \
        .mode("overwrite") \
        .saveAsTable("clinicalflow360.bronze.genomics_raw")

    # Verify
    count = spark.sql(
        "SELECT COUNT(*) FROM clinicalflow360.bronze.genomics_raw"
    ).collect()[0][0]

    print(f"✅ Bronze table created : clinicalflow360.bronze.genomics_raw")
    print(f"✅ Total records        : {count:,}")
    print(f"✅ Patients             : {df_pandas['USUBJID'].nunique():,}")
    print(f"✅ Sites                : {df_pandas['SITE_ID'].nunique():,}")


# Entry point
if __name__ == "__main__" or 'spark' in dir():
    run_bronze_ingestion()
