"""
Feature Engineering Script

This script connects to a PostgreSQL database and creates feature tables required for machine learning models. 
The features are derived from raw event data and include user activity, engagement, content completion rates, 
and other behavioral metrics. The final feature table is designed to support a "completion prediction" model.

Key Features:
1. Creates intermediate tables for session identification, user activity, and content engagement.
2. Aggregates data to compute user and content-level metrics such as completion rates and genre preferences.
3. Generates a final feature table (`completion_prediction_features`) with all relevant features for modeling.

Dependencies:
- psycopg2: For connecting to the PostgreSQL database.

Usage:
1. Ensure the `POSTGRES_PASSWORD` environment variable is set with the database password.
2. Run the script to create all feature tables in the database.

"""

import psycopg2
import os

def execute_sql_commands(conn, sql_commands):
    """
    Executes a list of SQL commands.

    Args:
        conn: psycopg2 connection object.
        sql_commands: A list of SQL command strings.
    """
    try:
        cur = conn.cursor()
        for sql_command in sql_commands:
            cur.execute(sql_command)
        conn.commit()
        print("Successfully executed all SQL commands.")
    except psycopg2.Error as e:
        print(f"Error executing SQL commands: {e}")
        conn.rollback()
    finally:
        if conn:
            cur.close()

def create_feature_tables(conn):
    """
    Creates the feature tables in PostgreSQL.

    Args:
        conn: psycopg2 connection object.
    """
    sql_commands = [
        """
        -- 1. Table: session_starts
        CREATE TABLE IF NOT EXISTS session_starts AS
        SELECT
            event_id,
            event_ts,
            user_id,
            content_id,
            LAG(event_ts, 1, 0) OVER (PARTITION BY user_id, content_id ORDER BY event_ts) AS prev_ts
        FROM
            enriched_table;
        """,
        """
        -- 2. Table: session_identification
        CREATE TABLE IF NOT EXISTS session_identification AS
        SELECT
            e.*,
            CASE
                WHEN ss.prev_ts = 0 OR e.event_ts - ss.prev_ts > 30 * 60 * 1000 THEN e.event_ts
                ELSE LAG(ss.event_ts, 1) OVER (PARTITION BY e.user_id, e.content_id ORDER BY e.event_ts)
            END AS session_start_ts
        FROM
            enriched_table e
        JOIN session_starts ss ON e.event_id = ss.event_id;
        """,
        """
        -- 3. Table: content_completion
        CREATE TABLE IF NOT EXISTS content_completion AS
        SELECT
            user_id,
            content_id,
            session_start_ts,
            MAX(event_ts) AS session_end_ts,
            MAX(CASE WHEN event_type = 'complete' THEN 1 ELSE 0 END) AS is_completed_event,
            MAX(CASE WHEN event_type = 'stop' AND CAST(playback_position AS FLOAT) / runtime >= 0.95 THEN 1 ELSE 0 END) AS stopped_near_end,
            COUNT(DISTINCT event_type) AS num_unique_events
        FROM
            session_identification
        GROUP BY
            user_id,
            content_id,
            session_start_ts;
        """,
        """
        -- 4. Table: user_activity
        CREATE TABLE IF NOT EXISTS user_activity AS
        SELECT
            e.event_id,
            e.user_id,
            e.event_ts,
            COUNT(prev_e.event_id) AS user_activity_level
        FROM
            enriched_table e
        LEFT JOIN
            enriched_table prev_e ON e.user_id = prev_e.user_id
                                AND prev_e.event_ts < e.event_ts
                                AND prev_e.event_ts >= (CAST(e.event_ts AS BIGINT) - 2592000000)
        GROUP BY
            e.event_id, e.user_id, e.event_ts;
        """,
        """
        -- 5. Table: user_engagement
        CREATE TABLE IF NOT EXISTS user_engagement AS
        SELECT
            e.event_id,
            e.user_id,
            e.event_ts,
            AVG(CAST(prev_e.playback_position AS FLOAT) / c.runtime) AS user_content_engagement
        FROM
            enriched_table e
        LEFT JOIN
            enriched_table prev_e ON e.user_id = prev_e.user_id
                                AND prev_e.event_ts < e.event_ts
                                AND prev_e.event_ts >= (e.event_ts - CAST(30 * 24 * 60 * 60 AS BIGINT) * 1000)
        JOIN
            content_dim c ON prev_e.content_id = c.content_id
        GROUP BY
            e.event_id, e.user_id, e.event_ts;
        """,
        """
        -- 6. Table: user_genre_preference
        CREATE TABLE IF NOT EXISTS user_genre_preference AS
        SELECT
            e.event_id,
            e.user_id,
            e.event_ts,
            (
                SELECT cd.genre
                FROM enriched_table prev_e
                JOIN content_dim cd ON prev_e.content_id = cd.content_id
                WHERE prev_e.user_id = e.user_id
                AND prev_e.event_ts < e.event_ts
                AND prev_e.event_ts >= (CAST(e.event_ts AS BIGINT) - 2592000000)
                GROUP BY cd.genre
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS user_genre_preference
        FROM
            enriched_table e;
        """,
        """
        -- 7. Table: user_completion_rate
        CREATE TABLE IF NOT EXISTS user_completion_rate AS
        SELECT
            e.event_id,
            e.user_id,
            AVG(CASE WHEN cc.is_completed_event = 1 OR cc.stopped_near_end = 1 THEN 1 ELSE 0 END) AS user_completion_rate
        FROM
            enriched_table e
        LEFT JOIN
            content_completion cc ON e.user_id = cc.user_id AND cc.session_end_ts < e.event_ts
        GROUP BY
            e.event_id, e.user_id;
        """,
        """
        -- 8. Table: content_completion_rate
        CREATE TABLE IF NOT EXISTS content_completion_rate AS
        SELECT
            e.event_id,
            e.content_id,
            AVG(CASE WHEN cc.is_completed_event = 1 OR cc.stopped_near_end = 1 THEN 1 ELSE 0 END) AS content_completion_rate
        FROM
            enriched_table e
        LEFT JOIN
            content_completion cc ON e.content_id = cc.content_id AND cc.session_end_ts < e.event_ts
        GROUP BY
            e.event_id, e.content_id;
        """,
        """
        -- 9. Table: avg_view_duration_ratio
        CREATE TABLE IF NOT EXISTS avg_view_duration_ratio AS
        SELECT
            e.event_id,
            e.content_id,
            AVG(CASE WHEN prev_e.event_type = 'stop' THEN CAST(prev_e.playback_position AS FLOAT) / cd.runtime ELSE NULL END) AS average_viewing_duration_ratio
        FROM
            enriched_table e
        LEFT JOIN
            enriched_table prev_e ON e.content_id = prev_e.content_id AND prev_e.event_ts < e.event_ts AND prev_e.event_type = 'stop'
        JOIN
            content_dim cd ON e.content_id = cd.content_id
        GROUP BY
            e.event_id, e.content_id;
        """,
        """
        -- 10. Table: has_played_before
        CREATE TABLE IF NOT EXISTS has_played_before AS
        SELECT
            e.event_id,
            e.user_id,
            e.content_id,
            CASE WHEN EXISTS (
                SELECT 1
                FROM enriched_table prev_e
                WHERE prev_e.user_id = e.user_id
                  AND prev_e.content_id = e.content_id
                  AND prev_e.event_ts < e.event_ts
            ) THEN 1 ELSE 0 END AS has_played_before
        FROM
            enriched_table e;
        """,
        """
        -- 11. Table: number_of_pauses
        CREATE TABLE IF NOT EXISTS number_of_pauses AS
        SELECT
            e.event_id,
            e.user_id,
            e.content_id,
            COUNT(prev_e.event_id) AS number_of_pauses
        FROM
            enriched_table e
        LEFT JOIN
            enriched_table prev_e ON e.user_id = prev_e.user_id
                                   AND e.content_id = prev_e.content_id
                                   AND prev_e.event_ts <= e.event_ts
                                   AND prev_e.event_type = 'pause'
                LEFT JOIN session_identification si_e ON e.event_id = si_e.event_id
                LEFT JOIN session_identification si_prev_e ON prev_e.event_id = si_prev_e.event_id
        WHERE si_prev_e.session_start_ts = si_e.session_start_ts
        GROUP BY
            e.event_id, e.user_id, e.content_id;
        """,
        """
        -- 12. Create the final feature table
        CREATE TABLE IF NOT EXISTS completion_prediction_features AS
        SELECT
            e.event_id,
            e.event_ts,
            u_dim.plan_tier AS user_plan_tier,
            CAST(FLOOR( (CAST(e.event_ts AS NUMERIC(20,0)) - CAST(EXTRACT(EPOCH FROM u_dim.signup_date) AS NUMERIC(20,0)) * 1000) / (24 * 60 * 60 * 1000)) AS BIGINT) AS days_since_signup,
            ua.user_activity_level,
            ue.user_content_engagement,
            ugp.user_genre_preference,
            ucr.user_completion_rate,
            c_dim.genre AS content_genre,
            c_dim.runtime AS content_runtime,
            c_dim.release_year AS content_release_year,
            c_dim.maturity_rating AS content_maturity_rating,
            EXTRACT(YEAR FROM CURRENT_DATE AT TIME ZONE 'UTC') - c_dim.release_year AS content_age,
            ccr.content_completion_rate,
            avdr.average_viewing_duration_ratio,
            e.event_type,
            CAST(e.playback_position AS FLOAT) / c_dim.runtime AS playback_position_ratio,
            e.device,
            EXTRACT(HOUR FROM TO_TIMESTAMP(e.event_ts / 1000) AT TIME ZONE 'UTC') AS hour_of_day,
            EXTRACT(DOW FROM TO_TIMESTAMP(e.event_ts / 1000) AT TIME ZONE 'UTC') AS day_of_week,
            (e.event_ts - LAG(e.event_ts, 1, 0) OVER (PARTITION BY e.user_id, e.content_id ORDER BY e.event_ts)) AS time_since_last_event,
            hpb.has_played_before,
            nop.number_of_pauses,
            CASE
                WHEN cc_final.is_completed_event = 1 THEN 1
                WHEN cc_final.stopped_near_end = 1 AND cc_final.num_unique_events > 1 THEN 1
                ELSE 0
            END AS is_completion
        FROM
            enriched_table e
        JOIN
            users_dim u_dim ON e.user_id = u_dim.user_id
        JOIN
            content_dim c_dim ON e.content_id = c_dim.content_id
        LEFT JOIN
            user_activity ua ON e.event_id = ua.event_id
        LEFT JOIN
            user_engagement ue ON e.event_id = ue.event_id
        LEFT JOIN
            user_genre_preference ugp ON e.event_id = ugp.event_id
        LEFT JOIN
            user_completion_rate ucr ON e.event_id = ucr.event_id
        LEFT JOIN
            content_completion_rate ccr ON e.event_id = ccr.event_id
        LEFT JOIN
            avg_view_duration_ratio avdr ON e.event_id = avdr.event_id
        LEFT JOIN
            has_played_before hpb ON e.event_id = hpb.event_id
        LEFT JOIN
            number_of_pauses nop ON e.event_id = nop.event_id
        LEFT JOIN
            session_identification si_final ON e.event_id = si_final.event_id
        LEFT JOIN
            content_completion cc_final ON e.user_id = cc_final.user_id
                                       AND e.content_id = cc_final.content_id
                                       AND si_final.session_start_ts = cc_final.session_start_ts
        ORDER BY
            e.event_ts;
        """
    ]
    execute_sql_commands(conn, sql_commands)

if __name__ == "__main__":
    # Database connection details
    db_host = 'pg-20c7c62-llmtravel.i.aivencloud.com'
    db_port = 21732
    db_name = 'defaultdb'
    db_user = 'avnadmin'
    db_password = os.environ.get('POSTGRES_PASSWORD')
    db_sslmode = 'require'

    conn = None
    try:
        if db_password is None:
            raise EnvironmentError("POSTGRES_PASSWORD environment variable not set.")

        db_connection_string = f"postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={db_sslmode}"
        conn = psycopg2.connect(db_connection_string)
        create_feature_tables(conn)
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
    except EnvironmentError as e:
        print(f"Configuration Error: {e}")
    finally:
        if conn:
            conn.close()
            print("PostgreSQL connection closed.")
