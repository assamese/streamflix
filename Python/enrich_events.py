import psycopg2
import os

def create_enriched_table(conn):
    """
    Executes the SQL code to create the enriched_table in the PostgreSQL database.

    Args:
        conn: psycopg2 connection object.
    """
    try:
        cur = conn.cursor()

        sql_create_enriched_table = """
        CREATE TABLE IF NOT EXISTS enriched_table AS
        SELECT
            e.event_id,
            e.event_ts,
            u.user_id AS user_id,
            u.email,
            u.plan_tier,
            u.signup_date,
            c.content_id AS content_id,
            c.genre,
            c.runtime,
            c.release_year,
            c.maturity_rating,
            e.event_type,
            e.playback_position,
            e.device
        FROM
            cleaned_events e
        JOIN
            users_dim u ON e.user_id = u.user_id
        JOIN
            content_dim c ON e.content_id = c.content_id;
        """

        cur.execute(sql_create_enriched_table)
        conn.commit()
        print("Successfully created the enriched_table.")

    except psycopg2.Error as e:
        print(f"Error creating the enriched_table: {e}")
        conn.rollback()
    finally:
        if conn:
            cur.close()

if __name__ == "__main__":
    # Database connection details
    db_host = 'pg-20c7c62-llmtravel.i.aivencloud.com'
    db_port = 21732
    db_name = 'defaultdb'
    db_user = 'avnadmin'
    db_password = os.environ.get('POSTGRES_PASSWORD')  # Read password from environment variable
    db_sslmode = 'require'

    conn = None
    try:
        if db_password is None:
            raise EnvironmentError("POSTGRES_PASSWORD environment variable not set.")

        db_connection_string = f"postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={db_sslmode}"
        conn = psycopg2.connect(db_connection_string)
        create_enriched_table(conn)
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
    except EnvironmentError as e:
        print(f"Configuration Error: {e}")
    finally:
        if conn:
            conn.close()
            print("PostgreSQL connection closed.")