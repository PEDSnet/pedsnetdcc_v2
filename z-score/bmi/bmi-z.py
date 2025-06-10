import time
import ibis
from ibis import row_number

from airflow.decorators import task
from contextlib import closing, contextmanager
from typing import Optional

conn_id = "loading_user_pbd02"
pg_user_template = "{{ conn['%s'].login }}" % conn_id
pg_password_template = "{{ conn['%s'].password }}" % conn_id
pg_host_template = "{{ conn['%s'].host }}" % conn_id
pg_database_template = "pedsnet_dcc_{{ dag_run.conf['submission'] }}"
# pg_database_template = "pedsnet_dcc_v51_airflow"
pcornet_conn_id = "loading_user_db06"
pcornet_pg_user_template = "{{ conn['%s'].login }}" % pcornet_conn_id
pcornet_pg_password_template = "{{ conn['%s'].password }}" % pcornet_conn_id
pcornet_pg_host_template = "{{ conn['%s'].host }}" % pcornet_conn_id
pcornet_pg_database_template = "pcornet_dcc_{{ dag_run.conf['submission'] }}"

submission_long_name_template = (
    "{{dag_run.conf['submission'][1:-1]}}.{{dag_run.conf['submission'][-1]}}.0"
)

ssh_conn_id = "ssh_shenq"
sudo_password_template = "{{ conn['%s'].password }}" % ssh_conn_id


@contextmanager
def ibis_connection(*args, **kwargs):
    con = ibis.postgres.connect(*args, **kwargs)
    try:
        yield con
    finally:
        print("Disposing Ibis connection…")
        try:
            con.close()
        except AttributeError:
            con.con.close()

def lookup_lms_bmi_age_based(con, meas, read_database):
    """
    Look up and linearly interpolate L, M, S for each (age, gender) row in meas
    by doing two lateral joins + ROW_NUMBER windows to pick the nearest lower
    and upper ages.
    """
    # 1) read & union the LMS tables once
    lms_f = con.table("bmi_child_female", database=read_database).select(
        age=ibis._.age,
        gender=ibis.literal("female"),
        l=ibis._.l,
        m=ibis._.m,
        s=ibis._.s,
    )
    lms_m = con.table("bmi_child_male", database=read_database).select(
        age=ibis._.age,
        gender=ibis.literal("male"),
        l=ibis._.l,
        m=ibis._.m,
        s=ibis._.s,
    )
    lms = lms_f.union(lms_m)

    # 2) LOWER bound branch (age ≤ measurement_age_in_months)
    j_lower = meas.left_join(
        lms, [(meas.gender == lms.gender) & (lms.age <= meas.age_in_months)]
    )

    w_lower = dict(
        group_by=[j_lower["measurement_id"]],
        order_by=[ibis.desc(j_lower["age"])],
    )
    ranked_lower = j_lower.mutate(rn=row_number().over(**w_lower))

    lower = ranked_lower.filter(ranked_lower["rn"] == 1).select(
        *[ranked_lower[c] for c in meas.columns],
        ranked_lower["age"].name("lower_age"),
        ranked_lower["l"].name("l_lo"),
        ranked_lower["m"].name("m_lo"),
        ranked_lower["s"].name("s_lo"),
    )

    # 3) UPPER bound branch (age ≥ measurement_age_in_months)
    j_upper = meas.left_join(
        lms, [(meas.gender == lms.gender) & (lms.age >= meas.age_in_months)]
    )

    w_upper = dict(
        group_by=[j_upper["measurement_id"]],
        order_by=[j_upper["age"]],
    )
    ranked_upper = j_upper.mutate(rn=row_number().over(**w_upper))

    upper = ranked_upper.filter(ranked_upper["rn"] == 1).select(
        *[ranked_upper[c] for c in meas.columns],
        ranked_upper["age"].name("upper_age"),
        ranked_upper["l"].name("l_hi"),
        ranked_upper["m"].name("m_hi"),
        ranked_upper["s"].name("s_hi"),
    )

    # 4) join LOWER + UPPER back together on your measurement key
    both = lower.inner_join(
        upper,
        predicates=[
            "measurement_id",
            "person_id",
            "measurement_datetime",
            "site_id",
            "provider_id",
            "visit_occurrence_id",
            "gender",
            "bmi_value",
            "age_in_months",
        ],
    )

    # 5) build interpolation CASE expressions
    frac = ibis.cases(
        ((both.upper_age == both.lower_age), 0.0),
        else_=(both.age_in_months - both.lower_age) / (both.upper_age - both.lower_age),
    )

    l = ibis.cases(
        ((both.upper_age == both.lower_age), both.l_lo),
        else_=both.l_lo + frac * (both.l_hi - both.l_lo),
    )
    m = ibis.cases(
        ((both.upper_age == both.lower_age), both.m_lo),
        else_=both.m_lo + frac * (both.m_hi - both.m_lo),
    )
    s = ibis.cases(
        ((both.upper_age == both.lower_age), both.s_lo),
        else_=both.s_lo + frac * (both.s_hi - both.s_lo),
    )

    return both, l, m, s


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

    z_score_expr = ibis.cases(
        # If lambda != 0, use standard LMS formula
        (
            meas_with_lms.l != 0,
            ((meas_with_lms.bmi_value / meas_with_lms.m) ** meas_with_lms.l - 1)
            / (meas_with_lms.l * meas_with_lms.s),
        ),
        else_=(meas_with_lms.bmi_value / meas_with_lms.m).log() / meas_with_lms.s,
    )

    # First, create the table with the z_score column
    with_z_score = meas_with_lms.mutate(z_score=z_score_expr)

    # Then filter using the newly created table that HAS the z_score column
    result = with_z_score.filter(
        [
            with_z_score.bmi_value > 0,
            with_z_score.m > 0,
            with_z_score.s > 0,
            with_z_score.z_score.notnull(),
            with_z_score.z_score.abs() <= 1e15,
        ]
    )

    # Calculate and display computation time
    end_time = time.time()
    total_seconds = end_time - start_time
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = total_seconds % 60

    print(f"Z-score calculation completed in {hours:02d}:{minutes:02d}:{seconds:06.3f}")

    return result


def cleanup_lms_tables(con, female_temp, male_temp: bool = False):
    """
    Clean up temporary LMS tables. Since they use temp=True, this is mostly for explicit cleanup.
    """
    if female_temp and male_temp:
        print("Cleaning up temporary LMS tables...")
        for temp_table in [female_temp, male_temp]:
            try:
                # Drop temp tables without database parameter
                con.drop_table(temp_table)
                print(f"  Dropped temporary table: {temp_table}")
            except Exception as e:
                print(
                    f"  Note: Could not drop {temp_table} (may have been auto-dropped): {e}"
                )


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
    db_backend: str = "postgres",
    trino_catalog: Optional[str] = None,
    trino_schema: Optional[str] = None,
) -> None:
    """
    Connects to Postgres or Trino via Ibis and computes BMI-for-age Z-scores.
    Uses temporary tables with temp=True for PostgreSQL compatibility.
    """
    with ibis_connection(
        host=pg_host,
        user=pg_user,
        password=pg_password,
        database=pg_database,
        port=5432,
    ) as con:
        read_database = f"{site}_pedsnet"
        write_database = read_database

        # Immediately elevate with proper cursor management:
        with closing(con.raw_sql(f"SET ROLE {role}")):
            pass
        # 3) If skip_calc, cleanup and return
        if skip_calc:
            # df = con.table(write_table, database=write_database)
            
            # df = df.value_as_number.cast("decimal(20, 10)").name("value_as_number")
            
            # print(f"skip_calc=True: Created and cleaned {write_database}.{write_table}")
            print(f"skip_calc=True is NOT YET IMPLEMENTED: did not clean {write_database}.{write_table}")
            return

        # 4) Connect to temporary LMS tables
        # female_temp = con.table("bmi_child_female", database=read_database)
        # male_temp = con.table("bmi_child_male", database=read_database)

        # 5) Build measurement query
        print("Building BMI measurement query...")

        m = con.table(read_table, database=read_database)
        p = con.table("person", database=read_database)

        print("Filtering BMI measurements...")

        # Main query to filter BMI measurements
        meas = (
            m.filter(
                [
                    m.measurement_concept_id
                    == measurement_concept_id,  # BMI Concept ID, '3038553' by default
                    m.value_as_number.notnull(),
                    m.unit_concept_id == 9531,  # kg/m^2 Concept ID
                    m.measurement_type_concept_id == 45754907,  # 'Derived' type
                    m.measurement_datetime.notnull(),
                ]
            )
            .inner_join(p, m.person_id == p.person_id)
            .filter(
                [
                    p.gender_concept_id.isin([8507, 8532]),  # male/female Concept IDs
                    p.birth_datetime.notnull(),
                ]
            )
            .mutate(
                bmi_value=m.value_as_number,
                bmi_source_value=m.value_source_value,
                age_in_months=(
                    m.measurement_datetime.delta(p.birth_datetime, unit="day")
                    / days_per_month
                ),  # Using delta to calculate age in months
                gender=ibis.cases(
                    (p.gender_concept_id == 8507, "male"),
                    (p.gender_concept_id == 8532, "female"),
                    else_=None,
                ),
            )
            .select(
                [
                    m.measurement_id,
                    m.person_id,
                    "bmi_value",
                    "bmi_source_value",
                    "age_in_months",
                    "gender",
                    m.measurement_datetime,
                    p.birth_datetime,
                    m.site_id,
                    m.provider_id,
                    m.visit_occurrence_id,
                ]
            )
        )

        # 6) Perform LMS lookup and Z-score calculation
        # assume `meas` is your Ibis TableExpr with columns `age_in_months` and `gender`
        both, l, m, s = lookup_lms_bmi_age_based(
            con, meas=meas, read_database=read_database
        )
        print("Performing LMS lookup and Z-score calculation...")

        # First attach the LMS parameters to the table
        meas_with_lms = both.mutate(l=l, m=m, s=s)

        # then compute z_score using the table columns
        z = ibis.cases(
            (
                (meas_with_lms.l != 0),
                ((meas_with_lms.bmi_value / meas_with_lms.m) ** meas_with_lms.l - 1)
                / (meas_with_lms.l * meas_with_lms.s),
            ),
            else_=(meas_with_lms.bmi_value / meas_with_lms.m).log() / meas_with_lms.s,
        )

        df = meas_with_lms.mutate(z_score=z).filter(z.notnull())

        # 8) Prepare and insert new rows (no idempotent deletion needed since table was recreated)
        if hasattr(df, "schema"):
            # Instead of copying to pandas, build the final expression in Ibis
            final_expr = df.mutate(
                measurement_concept_id=ibis.literal(zscore_concept_id),
                measurement_date=df.measurement_datetime.cast("date"),
                measurement_type_concept_id=ibis.literal(45754907),
                unit_concept_id=ibis.literal(0),
                unit_source_value=ibis.literal("SD"),
                value_as_number=df.z_score,
                value_source_value=ibis.literal("measurement: ").concat(  # type: ignore
                    df.bmi_source_value.fill_null("")
                ),
                measurement_source_value=ibis.literal(
                    f"PEDSnet NHANES 2000 Z score computation {version}"
                ),
                measurement_source_concept_id=ibis.literal(0),
                measurement_age_in_months=df.age_in_months,
            ).select(
                [
                    "measurement_id",
                    "person_id",
                    "measurement_concept_id",
                    "measurement_date",
                    "measurement_datetime",
                    "measurement_type_concept_id",
                    "unit_concept_id",
                    "unit_source_value",
                    "value_as_number",
                    "value_source_value",
                    "measurement_source_value",
                    "measurement_source_concept_id",
                    "measurement_age_in_months",
                    "site_id",
                    "provider_id",
                    "visit_occurrence_id",
                ]
            )

            start_time = time.time()

            print(
                f"Inserting new Z-score records into {write_database}.{write_table}..."
            )

            try:
                if compile:
                    print("\n" + "="*60)
                    print(f"Final expression for {write_database}.{write_table}:")
                    print("="*60 + "\n")
                    print(final_expr.compile(pretty=True))
                
                con.create_table(
                    write_table, final_expr, database=write_database, overwrite=True
                )
                print(f"Successfully created table {write_database}.{write_table}")
            except Exception as e:
                print(f"Error creating table {write_database}.{write_table}: {e}")
                print("Continuing with execution...")

            end_time = time.time()

            total_seconds = end_time - start_time
            hours = int(total_seconds // 3600)
            minutes = int((total_seconds % 3600) // 60)
            seconds = total_seconds % 60
            print(
                f"Table creation attempt completed in {hours:02d}:{minutes:02d}:{seconds:06.3f}"
            )

        print(f"Completed BMI Z-score calculation for site {site}")


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
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test database connection and exit",
    )
    parser.add_argument(
        "--db-backend",
        choices=["postgres", "trino"],
        default=os.getenv("DB_BACKEND", "postgres"),
        help="Database backend to use",
    )

    # Setup CLI arguments
    parser.add_argument(
        "--pg-user", help="PostgreSQL/Trino username", default=os.getenv("PG_USER")
    )
    parser.add_argument(
        "--pg-password",
        help="PostgreSQL/Trino password",
        default=os.getenv("PG_PASSWORD"),
    )
    parser.add_argument(
        "--pg-host", help="PostgreSQL/Trino host", default=os.getenv("PG_HOST")
    )
    parser.add_argument(
        "--pg-database",
        help="PostgreSQL database name",
        default=os.getenv("PG_DATABASE"),
    )
    parser.add_argument(
        "--site", help="Site name", default=os.getenv("SITE", "pedsnet")
    )
    parser.add_argument(
        "--skip-calc",
        help="Skip calculation, only run cleanup",
        action="store_true",
        default=os.getenv("SKIP_CALC", "false").lower() == "true",
    )
    parser.add_argument(
        "--compile",
        help="Compile the Ibis expression without executing",
        action="store_true",
        default=os.getenv("COMPILE", "false").lower() == "true",
    )
    parser.add_argument(
        "--trino-catalog", help="Trino catalog name", default=os.getenv("TRINO_CATALOG")
    )
    parser.add_argument(
        "--trino-schema", help="Trino schema name", default=os.getenv("TRINO_SCHEMA")
    )

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
    compile_ = args.compile

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

    compute_bmi_zscore_main(
        pg_user=pg_user,
        pg_password=pg_password,
        pg_host=pg_host,
        pg_database=pg_database,
        site=site,
        skip_calc=skip_calc,
        db_backend=args.db_backend,
        compile=compile_,
        trino_catalog=trino_catalog,
        trino_schema=trino_schema,
    )
