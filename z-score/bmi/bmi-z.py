import time
import uuid
import ibis
import numpy as np
from data.tables import weight_for_age_male_df, weight_for_age_female_df
import pandas as pd
pd.options.mode.copy_on_write = True 

from airflow.decorators import task
from contextlib import closing

# SQL templates for post-processing (only for operations not available in Ibis)
DROP_NULL_Z_TABLE_SQL = 'ALTER TABLE {0}.{1} ALTER COLUMN measurement_id DROP NOT NULL;'
BMIZ_INCREASE_VALUE_AS_NUMBER = 'ALTER TABLE {0}.{1} ALTER COLUMN value_as_number TYPE NUMERIC(25,5);'
BMIZ_DEFAULT_VALUE_AS_NUMBER = 'ALTER TABLE {0}.{1} ALTER COLUMN value_as_number TYPE NUMERIC(20,5);'

conn_id = 'loading_user_pbd02'
pg_user_template =  "{{ conn['%s'].login }}" %conn_id
pg_password_template =  "{{ conn['%s'].password }}" %conn_id
pg_host_template =  "{{ conn['%s'].host }}" %conn_id
pg_database_template = "pedsnet_dcc_{{ dag_run.conf['submission'] }}"
#pg_database_template = "pedsnet_dcc_v51_airflow"
pcornet_conn_id = 'loading_user_db06'
pcornet_pg_user_template =  "{{ conn['%s'].login }}" %pcornet_conn_id
pcornet_pg_password_template =  "{{ conn['%s'].password }}" %pcornet_conn_id
pcornet_pg_host_template =  "{{ conn['%s'].host }}" %pcornet_conn_id
pcornet_pg_database_template = "pcornet_dcc_{{ dag_run.conf['submission'] }}"

submission_long_name_template = "{{dag_run.conf['submission'][1:-1]}}.{{dag_run.conf['submission'][-1]}}.0"

ssh_conn_id = 'ssh_shenq'
sudo_password_template = "{{ conn['%s'].password }}" %ssh_conn_id

def create_write_table(con, read_table: str, write_table: str, database: str) -> None:
    """
    Drop existing write_table if it exists, then create an empty one LIKE read_table.
    """
    try:
        if write_table in con.list_tables(database=database):
            con.drop_table(write_table, database=database)
        template = con.table(read_table, database=database)
        con.create_table(write_table, database=database, schema=template.schema())
    except Exception as e:
        if "permission denied" in str(e).lower():
            raise PermissionError(f"Permission denied while creating or dropping table '{write_table}' in schema '{database}'. Please check your database permissions.") from e
        else:
            raise


def run_cleanup_steps(con, write_database: str, write_table: str) -> None:
    """
    Run all cleanup steps that should happen after Z-score calculation.
    """
    print("Running cleanup steps...")
    
    # Use context managers for all raw_sql calls
    with closing(con.raw_sql(DROP_NULL_Z_TABLE_SQL.format(write_database, write_table))):
        pass
    
    with closing(con.raw_sql(BMIZ_INCREASE_VALUE_AS_NUMBER.format(write_database, write_table))):
        pass
    
    table = con.table(write_table, database=write_database)
    table = table.filter([
        ~table.value_as_number.abs().round() > 1e15, 
        ~table.value_as_number.isnull(), 
        ~table.value_as_number.isnan()
        ]
    )
    
    with closing(con.raw_sql(BMIZ_DEFAULT_VALUE_AS_NUMBER.format(write_database, write_table))):
        pass
    
    nan_condition = table.value_as_number.isnan()
    table.filter(~nan_condition)
    print("Cleanup steps completed.")


def generate_temp_name(base_name: str, site: str) -> str:
    """Generate a unique temporary table name with site and timestamp."""
    process = "bmi_z_score"
    timestamp = str(int(time.time()))
    unique_id = str(uuid.uuid4())[:8]  # Short unique identifier
    return f"{process}_{base_name}_{site}_{timestamp}_{unique_id}"

def create_lms_tables(con, site: str, dry_run: bool = False):
    """
    Create temporary LMS tables using temp=True without specifying database/schema.
    PostgreSQL will automatically place them in the temporary schema.
    
    Returns:
        tuple: (female_temp_name, male_temp_name) if not dry_run, else (None, None)
    """
    print(f"{'DRY RUN: ' if dry_run else ''}Creating temporary LMS tables...")
    
    # Load hardcoded LMS tables
    female = weight_for_age_female_df.copy()
    male = weight_for_age_male_df.copy()
    
    # Create unique temporary table names
    female_temp = generate_temp_name("female_lms", site)
    male_temp = generate_temp_name("male_lms", site)
    
    if not dry_run:
        # Create temp tables with gender column - NO database parameter for temp tables
        female_with_gender = female.copy()
        female_with_gender['gender'] = 'female'
        male_with_gender = male.copy()
        male_with_gender['gender'] = 'male'
        
        # Create temporary tables with temp=True and NO database parameter
        con.create_table(female_temp, obj=female_with_gender, temp=True)
        con.create_table(male_temp, obj=male_with_gender, temp=True)
        
        print(f"Created temporary LMS tables: {female_temp}, {male_temp}")
        return female_temp, male_temp
    else:
        print(f"DRY RUN: Would create temporary tables {female_temp} and {male_temp}")
        return None, None

def perform_lms_lookup(con, meas_table, female_temp: str, male_temp: str, site: str, **kwargs):
    """
    Perform LMS parameter lookup using SQL strings for complex operations.
    Uses temporary tables with temp=True (no database parameter).
    """
    print("Performing LMS lookup and interpolation...")
    
    # Create unique temporary table name
    temp_meas_name = generate_temp_name("meas_with_lms", site)
    
    # Execute the meas_table query and create temp table
    print("Creating temporary measurement table...")
    # Check if we should load from cache or save to cache
    cache_file = kwargs.get('cache_file', f"cache/meas_data_{site}.parquet")
    
    if cache_file and os.path.exists(cache_file):
        print(f"Loading cached measurement data from {cache_file}...")
        meas_data = pd.read_parquet(cache_file)
        print(f"Loaded {len(meas_data)} cached measurement records")
    else:
        print("Executing measurement query...")
        meas_data = meas_table.execute()
        
        # Save to cache if cache_file is specified and we're running from __main__
        if cache_file and __name__ == "__main__":
            print(f"Saving measurement data to cache file {cache_file}...")
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            meas_data.to_parquet(cache_file, index=False)
            print(f"Cached {len(meas_data)} measurement records to {cache_file}")
    
    print(f"Creating temporary measurement table: {temp_meas_name}")
    # Create temporary table with temp=True and NO database parameter
    con.create_table(temp_meas_name, obj=meas_data, temp=True)
    
    # For temporary tables, PostgreSQL places them in pg_temp schema
    # We can reference them without schema qualification in SQL
    lms_lookup_sql = f"""
    WITH lms_union AS (
        SELECT age, lambda as l, mean as m, sigma as s, gender 
        FROM {female_temp}
        UNION ALL
        SELECT age, lambda as l, mean as m, sigma as s, gender 
        FROM {male_temp}
    ),
    meas_with_bounds AS (
        SELECT 
            tm.*,
            -- Lower bound: highest LMS age <= measurement age
            (SELECT MAX(lms.age) 
             FROM lms_union lms 
             WHERE lms.gender = tm.gender 
               AND lms.age <= tm.age_in_months) as lower_age,
            -- Upper bound: lowest LMS age >= measurement age  
            (SELECT MIN(lms.age) 
             FROM lms_union lms 
             WHERE lms.gender = tm.gender 
               AND lms.age >= tm.age_in_months) as upper_age
        FROM {temp_meas_name} tm
    ),
    meas_with_lms AS (
        SELECT 
            mwb.*,
            -- Lower LMS parameters
            lms_lo.l as l_lo,
            lms_lo.m as m_lo, 
            lms_lo.s as s_lo,
            -- Upper LMS parameters
            lms_hi.l as l_hi,
            lms_hi.m as m_hi,
            lms_hi.s as s_hi
        FROM meas_with_bounds mwb
        LEFT JOIN lms_union lms_lo 
            ON lms_lo.gender = mwb.gender 
            AND lms_lo.age = mwb.lower_age
        LEFT JOIN lms_union lms_hi 
            ON lms_hi.gender = mwb.gender 
            AND lms_hi.age = mwb.upper_age
    ),
    interpolated AS (
        SELECT 
            *,
            -- Interpolation fraction
            CASE 
                WHEN upper_age = lower_age THEN 0.0
                ELSE (age_in_months - lower_age) / (upper_age - lower_age)
            END as frac,
            -- Interpolated L parameter
            CASE 
                WHEN upper_age = lower_age THEN l_lo
                ELSE l_lo + ((age_in_months - lower_age) / (upper_age - lower_age)) * (l_hi - l_lo)
            END as l,
            -- Interpolated M parameter  
            CASE 
                WHEN upper_age = lower_age THEN m_lo
                ELSE m_lo + ((age_in_months - lower_age) / (upper_age - lower_age)) * (m_hi - m_lo)
            END as m,
            -- Interpolated S parameter
            CASE 
                WHEN upper_age = lower_age THEN s_lo
                ELSE s_lo + ((age_in_months - lower_age) / (upper_age - lower_age)) * (s_hi - s_lo)
            END as s
        FROM meas_with_lms
        WHERE lower_age IS NOT NULL 
          AND upper_age IS NOT NULL
          AND l_lo IS NOT NULL 
          AND m_lo IS NOT NULL 
          AND s_lo IS NOT NULL
          AND l_hi IS NOT NULL 
          AND m_hi IS NOT NULL 
          AND s_hi IS NOT NULL
    )
    SELECT * FROM interpolated
    """
    
    print("Executing LMS lookup SQL query... This might take a moment.")
    # Execute the SQL and get result as Ibis table
    result_table = con.sql(lms_lookup_sql, dialect="postgres")
    print("LMS lookup SQL query executed successfully.")
    
    print("LMS lookup and interpolation completed.")
    
    # Return both the result table AND the temp table name for cleanup later
    return result_table, temp_meas_name

def calculate_z_scores(meas_with_lms, **kwargs):
    """
    Calculate Z-scores using the LMS method.
    
    Args:
        meas_with_lms: Ibis table expression with measurements and interpolated LMS parameters
        
    Returns:
        Ibis table expression with Z-scores calculated
    """
    print("Computing Z-scores with LMS method...")
    
    # Calculate Z-scores using LMS method
    start_time = time.time()
    print("Starting Z-score calculation...")
    
    # Unpack the tuple from perform_lms_lookup
    result_table, temp_meas_name = meas_with_lms
    
    z_score_expr = ibis.cases(
        # If lambda != 0, use standard LMS formula
        (result_table.l != 0, 
         ((result_table.bmi_value / result_table.m) ** result_table.l - 1) / (result_table.l * result_table.s)),
        else_=(result_table.bmi_value / result_table.m).log() / result_table.s
    )

    # First, create the table with the z_score column
    with_z_score = result_table.mutate(z_score=z_score_expr)
    
    # Then filter using the newly created table that HAS the z_score column
    result = (
        with_z_score
        .filter([
            with_z_score.bmi_value > 0, 
            with_z_score.m > 0, 
            with_z_score.s > 0,
            with_z_score.z_score.notnull(),
            with_z_score.z_score.abs() <= 1e15
        ])
    )
    
    # Execute the query to get results
    print("Executing Z-score calculation...")
    # Check if we should load from cache or save to cache
    cache_file = kwargs.get('cache_file', f"cache/bmi_z_scores_{site}.parquet")
    
    if cache_file and os.path.exists(cache_file):
        print(f"Loading cached results from {cache_file}...")
        df = pd.read_parquet(cache_file)
        print(f"Loaded {len(df)} cached Z-score records")
    else:
        print("Executing Z-score calculation...")
        df = result.execute()
        
        # Save to cache if cache_file is specified
        if cache_file:
            print(f"Saving results to cache file {cache_file}...")
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            df.to_parquet(cache_file, index=False)
            print(f"Cached {len(df)} records to {cache_file}")
    
    # Calculate and display computation time
    end_time = time.time()
    total_seconds = end_time - start_time
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = total_seconds % 60
    
    print(f"Z-score calculation completed in {hours:02d}:{minutes:02d}:{seconds:06.3f}")
    print(f"Calculated {len(df)} valid Z-scores")
    
    return df


def cleanup_lms_tables(con, female_temp: str, male_temp: str, dry_run: bool = False):
    """
    Clean up temporary LMS tables. Since they use temp=True, this is mostly for explicit cleanup.
    """
    if not dry_run and female_temp and male_temp:
        print("Cleaning up temporary LMS tables...")
        for temp_table in [female_temp, male_temp]:
            try:
                # Drop temp tables without database parameter
                con.drop_table(temp_table)
                print(f"  Dropped temporary table: {temp_table}")
            except Exception as e:
                print(f"  Note: Could not drop {temp_table} (may have been auto-dropped): {e}")
                
    elif dry_run:
        print(f"DRY RUN: Would drop temporary tables {female_temp} and {male_temp}")

def compute_bmi_zscore(**kwargs):
    compute_bmi_zscore_main(**kwargs)
    
def compute_bmi_zscore_main(
    pg_user: str = pg_user_template,
    pg_password: str = pg_password_template,
    pg_host: str = pg_host_template, 
    pg_database: str = pg_database_template,
    role: str = "dcc_owner",
    site: str = "{{dag_run.conf['site']}}",
    measurement_concept_id: int = 3038553,
    zscore_concept_id: int = 2000000043,
    version: str = "v1.0",
    read_table: str = "measurement_anthro",
    write_table: str = "measurement_bmiz_new_approach_ibis",
    days_per_month: float = 30.44,
    skip_calc: bool = False,
    dry_run: bool = False,
    db_backend: str = "postgres",
    trino_catalog: str = None,
    trino_schema: str = None,
) -> None:
    """
    Connects to Postgres or Trino via Ibis and computes BMI-for-age Z-scores.
    Uses temporary tables with temp=True for PostgreSQL compatibility.
    """
    
    read_database = f"{site}_pedsnet"
    write_database = read_database
    
    # 1) Establish Ibis connection
    if db_backend.lower() == "trino":
        if not trino_catalog or not trino_schema:
            raise ValueError("trino_catalog and trino_schema are required when using Trino backend")
        con = ibis.trino.connect(
            host=pg_host,
            user=pg_user,
            password=pg_password,
            catalog=trino_catalog,
            schema=trino_schema,
        )
    elif db_backend.lower() == "postgres":
        con = ibis.postgres.connect(
            host=pg_host,
            user=pg_user,
            password=pg_password,
            database=pg_database,
            port=5432,
        )
    else:
        raise ValueError(f"Unsupported db_backend: {db_backend}. Use 'postgres' or 'trino'")

    # Immediately elevate with proper cursor management:
    with closing(con.raw_sql(f"SET ROLE {role}")):
        pass

    # 2) Drop and recreate write table for idempotence (skip if dry run)
    if not dry_run:
        print(f"Ensuring idempotence: recreating {write_database}.{write_table}")
        create_write_table(con, read_table, write_table, write_database)
    else:
        print(f"DRY RUN: Would recreate table {write_database}.{write_table} like {read_database}.{read_table}")

    # 3) If skip_calc, cleanup and return
    if skip_calc:
        if not dry_run:
            run_cleanup_steps(con, write_database, write_table)
        else:
            print(f"DRY RUN: Would run cleanup steps on {write_database}.{write_table}")
        print(f"skip_calc=True: {'would create and clean' if dry_run else 'created and cleaned'} {write_database}.{write_table}")
        return

    # 4) Create temporary LMS tables
    female_temp, male_temp = create_lms_tables(con, site, dry_run)

    # 5) Build measurement query
    print(f"{'DRY RUN: ' if dry_run else ''}Building BMI measurement query...")
    
    m = con.table(read_table, database=read_database)
    p = con.table("person", database=read_database)

    meas = (
        m
        .filter([
            m.measurement_concept_id == measurement_concept_id,
            m.value_as_number.notnull(),
            m.unit_concept_id == 9531,
            m.measurement_type_concept_id == 45754907,
        ])
        .inner_join(p, m.person_id == p.person_id)
        .filter(p.gender_concept_id.isin([8507, 8532]))
        .mutate(
            bmi_value=m.value_as_number,
            bmi_source_value=m.value_source_value,
            age_in_months=(
                m.measurement_datetime.delta(p.birth_datetime, unit="day") / days_per_month
            ),
            gender=ibis.cases(
                (p.gender_concept_id == 8507, "male"),
                (p.gender_concept_id == 8532, "female"),
                else_=None
            )
        )
        .select([
            m.measurement_id, m.person_id, "bmi_value", "bmi_source_value",
            "age_in_months", "gender", m.measurement_datetime, p.birth_datetime,
            m.site_id, m.provider_id, m.visit_occurrence_id
        ])
    )

    if not dry_run:
        # 6) Perform LMS lookup and Z-score calculation
        meas_with_lms = perform_lms_lookup(con, meas, female_temp, male_temp, site)
        df = calculate_z_scores(meas_with_lms, site = site)
        
        # Clean up temporary tables
        cleanup_lms_tables(con, female_temp, male_temp, dry_run)

    else:
        print("DRY RUN: Would execute Z-score calculation")
        df_sample = meas.limit(100).execute()
        print(f"DRY RUN: Would process approximately {len(df_sample)} records")
        df = df_sample
        cleanup_lms_tables(con, female_temp, male_temp, dry_run)

    # 7) Print statistics
    if not dry_run and len(df) > 0:
        original_count = meas.count().execute()
        filtered_count = len(df)
        
        print("\n" + "="*60)
        print("BMI Z-SCORE CALCULATION SUMMARY")
        print("="*60)
        print(f"Site: {site}")
        print(f"Total BMI records found: {original_count:,}")
        print(f"Valid Z-scores calculated: {filtered_count:,}")
        print(f"Success rate: {(filtered_count/original_count)*100:.1f}%")
        
        if 'z_score' in df.columns:
            z_scores = df['z_score']
            print(f"\nZ-score distribution:")
            print(f"  Min: {z_scores.min():.3f}")
            print(f"  Max: {z_scores.max():.3f}")
            print(f"  Mean: {z_scores.mean():.3f}")
            print(f"  Median: {z_scores.median():.3f}")
        print("="*60)

    # 8) Prepare and insert new rows (no idempotent deletion needed since table was recreated)
    if len(df) > 0:
        df_insert = df.copy()
        # df_insert['measurement_id'] = None
        df_insert['measurement_concept_id'] = zscore_concept_id
        df_insert['measurement_date'] = df_insert.measurement_datetime.dt.date
        df_insert['measurement_type_concept_id'] = 45754907
        # df_insert['unit_concept_id'] = 0
        df_insert['unit_source_value'] = 'SD'
        
        if 'z_score' in df_insert.columns:
            df_insert['value_as_number'] = df_insert['z_score']
        else:
            df_insert['value_as_number'] = 0.0  # Placeholder for dry run
            
        df_insert['value_source_value'] = (
            'measurement: ' + 
            df_insert.bmi_source_value.fillna('').astype(str)
        )
        df_insert['measurement_source_value'] = f"PEDSnet NHANES 2000 Z score computation {version}"
        # df_insert['measurement_source_concept_id'] = 0
        df_insert = df_insert.rename(columns={'age_in_months':'measurement_age_in_months'})
        
        cols = [
            'measurement_id','person_id','measurement_concept_id','measurement_date',
            'measurement_datetime','measurement_type_concept_id','unit_concept_id',
            'unit_source_value','value_as_number','value_source_value',
            'measurement_source_value','measurement_source_concept_id',
            'measurement_age_in_months','site_id','provider_id','visit_occurrence_id'
        ]
        df_insert = df_insert[cols]
        
        if not dry_run:
            start_time = time.time()
            con.insert(write_table, df_insert, database=write_database)
            end_time = time.time()
            total_seconds = end_time - start_time
            hours = int(total_seconds // 3600)
            minutes = int((total_seconds % 3600) // 60)
            seconds = total_seconds % 60
            print(f"Inserted {len(df_insert)} new Z-score records in {hours:02d}:{minutes:02d}:{seconds:06.3f}")
        else:
            print(f"DRY RUN: Would insert {len(df_insert)} new Z-score records")

    # 9) Final cleanup steps
    if not dry_run:
        run_cleanup_steps(con, write_database, write_table)
    else:
        print(f"DRY RUN: Would run cleanup steps on {write_database}.{write_table}")

    print(f"{'DRY RUN: Would complete' if dry_run else 'Completed'} BMI Z-score calculation for site {site}")

# CLI entrypoint with support for both Trino and Postgres
if __name__ == "__main__":
    import os
    import argparse
    import sys
    
    def check_required_packages():
        """Check if required packages are installed."""
        required_packages = ["ibis", "pandas", "numpy"]
        missing_packages = []
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                missing_packages.append(package)
        
        if missing_packages:
            print(f"Error: Missing required packages: {', '.join(missing_packages)}")
            sys.exit(1)
    
    # Check required packages first
    check_required_packages()
    
    parser = argparse.ArgumentParser(description="Compute BMI Z-scores")
    parser.add_argument("--test-connection", action="store_true", 
                       help="Test database connection and exit")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be done without making changes", default=False)
    parser.add_argument("--db-backend", choices=["postgres", "trino"], 
                       default=os.getenv("DB_BACKEND", "postgres"),
                       help="Database backend to use")
    
    # Setup CLI arguments
    parser.add_argument("--pg-user", help="PostgreSQL/Trino username", default=os.getenv("PG_USER"))
    parser.add_argument("--pg-password", help="PostgreSQL/Trino password", default=os.getenv("PG_PASSWORD"))
    parser.add_argument("--pg-host", help="PostgreSQL/Trino host", default=os.getenv("PG_HOST"))
    parser.add_argument("--pg-database", help="PostgreSQL database name", default=os.getenv("PG_DATABASE"))
    parser.add_argument("--site", help="Site name", default=os.getenv("SITE", "pedsnet"))
    parser.add_argument("--skip-calc", help="Skip calculation, only run cleanup", 
                        action="store_true", default=os.getenv("SKIP_CALC", "false").lower() == "true")
    parser.add_argument("--trino-catalog", help="Trino catalog name", default=os.getenv("TRINO_CATALOG"))
    parser.add_argument("--trino-schema", help="Trino schema name", default=os.getenv("TRINO_SCHEMA"))
    
    try:
        args = parser.parse_args()
    except Exception as e:
        print(f"Error parsing arguments: {e}")
        sys.exit(1)
    
    # Extract values from args with environment variables as fallbacks
    pg_user = args.pg_user
    pg_password = args.pg_password
    pg_host = args.pg_host
    pg_database = args.pg_database
    site = args.site
    skip_calc = args.skip_calc
    dry_run = args.dry_run
    
    # Check required arguments based on database backend
    missing_args = []
    
    # Common required arguments
    if not pg_user:
        missing_args.append("--pg-user")
    if not pg_password:
        missing_args.append("--pg-password")
    if not pg_host:
        missing_args.append("--pg-host")
    
    # Backend-specific requirements
    if args.db_backend == "postgres" and not pg_database:
        missing_args.append("--pg-database")
    elif args.db_backend == "trino":
        if not args.trino_catalog:
            missing_args.append("--trino-catalog")
        if not args.trino_schema:
            missing_args.append("--trino-schema")
    
    if missing_args:
        print(f"Error: Missing required arguments: {', '.join(missing_args)}")
        parser.print_help()
        sys.exit(1)
    
    # Backend-specific parameters
    trino_catalog = args.trino_catalog if args.db_backend == "trino" else None
    trino_schema = args.trino_schema if args.db_backend == "trino" else None
    
    # Test connection if requested
    if args.test_connection:
        try:
            if args.db_backend == "trino":
                import ibis
                con = ibis.trino.connect(
                    host=pg_host,
                    user=pg_user,
                    password=pg_password,
                    catalog=trino_catalog,
                    schema=trino_schema,
                )
                # Test with a simple query
                con.sql("SELECT 1").execute()
                print(f"Trino connection successful to {pg_host}")
            else:
                import ibis
                con = ibis.postgres.connect(
                    host=pg_host,
                    user=pg_user,
                    password=pg_password,
                    database=pg_database,
                    port=5432,
                )
                # Test with a simple query
                result = con.sql("SELECT 1").execute()
                print(f"Postgres connection successful to {pg_host}, result: {result}")
        except Exception as e:
            print(f"Connection failed: {e}")
            exit(1)
        exit(0)
    
    # Run the main computation
    compute_bmi_zscore_main(
        pg_user=pg_user,
        pg_password=pg_password,
        pg_host=pg_host,
        pg_database=pg_database,
        site=site,
        skip_calc=skip_calc,
        dry_run=dry_run,
        db_backend=args.db_backend,
        trino_catalog=trino_catalog,
        trino_schema=trino_schema
    )
