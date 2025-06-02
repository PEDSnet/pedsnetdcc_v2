import ibis
import numpy as np
from data.tables import weight_for_age_male_df, weight_for_age_female_df

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
    overflow_condition = table.value_as_number.abs().round() > 1e15
    table.filter(~overflow_condition).to_table(write_table, database=write_database, overwrite=True)
    
    with closing(con.raw_sql(BMIZ_DEFAULT_VALUE_AS_NUMBER.format(write_database, write_table))):
        pass
    
    table = con.table(write_table, database=write_database)
    nan_condition = table.value_as_number.isnan()
    table.filter(~nan_condition).to_table(write_table, database=write_database, overwrite=True)
    print("Cleanup steps completed.")


def create_lms_tables(con, write_database: str, dry_run: bool = False):
    """
    Create temporary LMS tables in the database from hardcoded data.
    
    Returns:
        tuple: (female_temp_name, male_temp_name) if not dry_run, else (None, None)
    """
    print(f"{'DRY RUN: ' if dry_run else ''}Creating temporary LMS tables in database...")
    
    # Load hardcoded LMS tables
    female = weight_for_age_female_df.copy()
    male = weight_for_age_male_df.copy()
    
    # Create temporary tables in database
    female_temp = "temp_female_lms"
    male_temp = "temp_male_lms"
    
    if not dry_run:
        # Drop existing temp tables if they exist
        for temp_table in [female_temp, male_temp]:
            if temp_table in con.list_tables(database=write_database):
                con.drop_table(temp_table, database=write_database)
        
        # Create temp tables with gender column
        female_with_gender = female.copy()
        female_with_gender['gender'] = 'female'
        male_with_gender = male.copy()
        male_with_gender['gender'] = 'male'
        
        con.create_table(female_temp, obj=female_with_gender, database=write_database)
        con.create_table(male_temp, obj=male_with_gender, database=write_database)
        
        return female_temp, male_temp
    else:
        print(f"DRY RUN: Would create temporary tables {female_temp} and {male_temp}")
        return None, None

def perform_lms_lookup(con, meas_table, female_temp: str, male_temp: str, write_database: str):
    """
    Perform LMS parameter lookup using SQL strings for complex operations.
    This approach avoids ambiguous field references by using explicit SQL.
    """
    print("Performing LMS lookup and interpolation...")
    
    # First, create a temporary table with the measurement data
    temp_meas_name = "temp_meas_with_lms"
    
    # Execute the meas_table query and create temp table
    print("Creating temporary measurement table...")
    meas_data = meas_table.execute()
    con.create_table(temp_meas_name, obj=meas_data, database=write_database, overwrite=True)
    
    # Use SQL to perform the complex LMS lookup operations
    print("Finding LMS parameters using SQL...")
    
    # Create the LMS lookup query using SQL
    lms_lookup_sql = f"""
    WITH lms_union AS (
        SELECT age, lambda as l, mean as m, sigma as s, gender 
        FROM {write_database}.{female_temp}
        UNION ALL
        SELECT age, lambda as l, mean as m, sigma as s, gender 
        FROM {write_database}.{male_temp}
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
        FROM {write_database}.{temp_meas_name} tm
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
    
    # Execute the SQL and get result as Ibis table
    result_table = con.sql(lms_lookup_sql)
    
    # Clean up the temporary measurement table
    con.drop_table(temp_meas_name, database=write_database)
    
    print("LMS lookup and interpolation completed.")
    return result_table

def calculate_z_scores(meas_with_lms):
    """
    Calculate Z-scores using the LMS method.
    
    Args:
        meas_with_lms: Ibis table expression with measurements and interpolated LMS parameters
        
    Returns:
        Ibis table expression with Z-scores calculated
    """
    print("Computing Z-scores with LMS method...")
    
    # Calculate Z-scores using LMS method
    z_score_expr = ibis.cases(
        # If lambda != 0, use standard LMS formula
        (meas_with_lms.l != 0, 
         ((meas_with_lms.bmi_value / meas_with_lms.m) ** meas_with_lms.l - 1) / (meas_with_lms.l * meas_with_lms.s)),
        # If lambda == 0, use log formula
        else_=(meas_with_lms.bmi_value / meas_with_lms.m).log() / meas_with_lms.s
    )

    result = (
        meas_with_lms
        .mutate(z_score=z_score_expr)
        .filter([
            meas_with_lms.bmi_value > 0, 
            meas_with_lms.m > 0, 
            meas_with_lms.s > 0,
            meas_with_lms.z_score.notnull(),
            meas_with_lms.z_score.abs() <= 1e15
        ])
    )
    
    return result


def cleanup_lms_tables(con, female_temp: str, male_temp: str, write_database: str, dry_run: bool = False):
    """
    Clean up temporary LMS tables.
    """
    if not dry_run and female_temp and male_temp:
        print("Cleaning up temporary LMS tables...")
        for temp_table in [female_temp, male_temp]:
            if temp_table in con.list_tables(database=write_database):
                con.drop_table(temp_table, database=write_database)
    elif dry_run:
        print(f"DRY RUN: Would drop temporary tables {female_temp} and {male_temp}")


@task.external_python(task_id="compute_bmi_zscore", python="../z-score_venv/bin/python")
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

    If skip_calc is True, runs only post-processing cleanup. Otherwise proceeds
    through full calculation, insertion, then cleanup.
    
    Args:
        dry_run: If True, performs all operations except table creation and data insertion
        db_backend: Either "postgres" or "trino"
        trino_catalog: Required if db_backend is "trino"
        trino_schema: Required if db_backend is "trino"
    """
    
    read_database = f"{site}_pedsnet"
    write_database = read_database
    
    # 1) Establish Ibis connection based on backend
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
        pass  # DDL statement, cursor closed automatically

    # 2) Prepare write table (skip if dry run)
    if not dry_run:
        create_write_table(con, read_table, write_table, write_database)
    else:
        print(f"DRY RUN: Would create table {write_database}.{write_table} like {read_database}.{read_table}")

    # 3) If skip_calc, cleanup and return
    if skip_calc:
        if not dry_run:
            run_cleanup_steps(con, write_database, write_table)
        else:
            print(f"DRY RUN: Would run cleanup steps on {write_database}.{write_table}")
        print(f"skip_calc=True: {'would create and clean' if dry_run else 'created and cleaned'} {write_database}.{write_table}")
        return

    # 4) Create temporary LMS tables in database from hardcoded data
    female_temp, male_temp = create_lms_tables(con, write_database, dry_run)

    # 5) Build Ibis expressions for BMI Z-score calculation
    print(f"{'DRY RUN: ' if dry_run else ''}Building BMI measurement query...")
    
    m = con.table(read_table, database=read_database)
    p = con.table("person", database=read_database)

    # Base measurement + age + gender calculation
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
        # 6) Perform LMS lookup and interpolation
        meas_with_lms = perform_lms_lookup(con, meas, female_temp, male_temp, write_database)
        
        # 7) Calculate Z-scores
        result = calculate_z_scores(meas_with_lms)

        # Execute the query to get results
        print("Executing Z-score calculation...")
        df = result.execute()
        print(f"Calculated {len(df)} valid Z-scores")

        # Clean up temporary tables
        cleanup_lms_tables(con, female_temp, male_temp, write_database, dry_run)

    else:
        print("DRY RUN: Would execute Z-score calculation with Ibis")
        # For dry run, just execute the measurement query to show what would be processed
        df_sample = meas.limit(100).execute()
        print(f"DRY RUN: Would process approximately {len(df_sample)} records (showing sample of 100)")
        df = df_sample  # Use sample for rest of dry run logic
        cleanup_lms_tables(con, female_temp, male_temp, write_database, dry_run)

    # 8) Print comprehensive statistics
    if not dry_run:
        # Get original count for comparison
        original_count = meas.count().execute()
        filtered_count = len(df)
        
        print("\n" + "="*60)
        print("IBIS DATABASE Z-SCORE CALCULATION SUMMARY")
        print("="*60)
        
        print(f"Total BMI records found: {original_count:,}")
        print(f"Valid Z-scores calculated: {filtered_count:,}")
        print(f"Invalid/filtered Z-scores: {original_count - filtered_count:,}")
        print(f"Success rate: {(filtered_count/original_count)*100:.1f}%")
        
        if filtered_count > 0:
            z_scores = df['z_score']
            print(f"\nZ-score distribution:")
            print(f"  Min: {z_scores.min():.3f}")
            print(f"  Max: {z_scores.max():.3f}")
            print(f"  Mean: {z_scores.mean():.3f}")
            print(f"  Median: {z_scores.median():.3f}")
            print(f"  Std Dev: {z_scores.std():.3f}")
            print(f"  25th percentile: {z_scores.quantile(0.25):.3f}")
            print(f"  75th percentile: {z_scores.quantile(0.75):.3f}")
            
            # Flag extreme Z-scores
            extreme_low = np.sum(z_scores < -5)
            extreme_high = np.sum(z_scores > 5)
            if extreme_low > 0 or extreme_high > 0:
                print(f"\nExtreme Z-scores (may indicate data issues):")
                if extreme_low > 0:
                    print(f"  Z < -5: {extreme_low:,} records")
                if extreme_high > 0:
                    print(f"  Z > 5: {extreme_high:,} records")
        
        print("="*60)

    # 9) Idempotent delete via Ibis anti-join if needed
    if not df.empty:
        if not dry_run:
            print("Deleting existing Z-score records for idempotence...")
            temp_df = df[['person_id','measurement_datetime']]
            temp_name = f"temp_del_{write_table}"
            con.create_table(temp_name, database=write_database, obj=temp_df, overwrite=True)
            targ = con.table(write_table, database=write_database)
            tmp = con.table(temp_name, database=write_database)
            keep = ~(
                (targ.measurement_concept_id == zscore_concept_id) &
                (targ.measurement_type_concept_id == 45754907) &
                targ.person_id.isin(tmp.person_id) &
                targ.measurement_datetime.isin(tmp.measurement_datetime)
            )
            targ.filter(keep).to_table(write_table, database=write_database, overwrite=True)
            con.drop_table(temp_name, database=write_database)
            print(f"Deleted existing Z-score records for {len(df)} measurements")
        else:
            print(f"DRY RUN: Would delete existing Z-score records for {len(df)} measurements")

    # 10) Prepare & insert new rows
    df_insert = df.copy()
    df_insert['measurement_id'] = None
    df_insert['measurement_concept_id'] = zscore_concept_id
    df_insert['measurement_date'] = df_insert.measurement_datetime.dt.date
    df_insert['measurement_type_concept_id'] = 45754907
    df_insert['unit_concept_id'] = 0
    df_insert['unit_source_value'] = 'SD'
    # Handle z_score column for both dry run and actual run
    if 'z_score' in df_insert.columns:
        df_insert['value_as_number'] = df_insert['z_score']
    else:
        df_insert['value_as_number'] = 0.0  # Placeholder for dry run
    df_insert['value_source_value'] = (
        'measurement: ' + df_insert.measurement_id.fillna('').astype(str)
        + ' ' + df_insert.bmi_source_value.fillna('')
    )
    df_insert['measurement_source_value'] = f"PEDSnet NHANES 2000 Z score computation {version}"
    df_insert['measurement_source_concept_id'] = 0
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
        con.insert(write_table, df_insert, database=write_database)
        print(f"Inserted {len(df_insert)} new Z-score records")
    else:
        print(f"DRY RUN: Would insert {len(df_insert)} new Z-score records")
        print("Sample of records that would be inserted:")
        print(df_insert.head().to_string())

    # 11) Always cleanup (skip if dry run)
    if not dry_run:
        run_cleanup_steps(con, write_database, write_table)
    else:
        print(f"DRY RUN: Would run cleanup steps on {write_database}.{write_table}")

    print(f"{'DRY RUN: Would write' if dry_run else 'Wrote'} and cleaned BMI Z-scores in {write_database}.{write_table}")

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
