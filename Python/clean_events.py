"""
Clean Events Script

This script reads raw event data from a PostgreSQL database, performs data cleaning and validation, 
and writes the cleaned data to a new table in the database. The cleaning process includes:
1. Validating data types and presence of required fields.
2. Filtering out invalid or incomplete events.
3. Converting timestamps and playback positions to integers.
4. Ensuring only valid event types are retained.

Key Features:
- Reads raw events from the `events_table`.
- Cleans and filters the data based on predefined rules.
- Writes the cleaned data to the `cleaned_events` table, avoiding duplicates.

Dependencies:
- psycopg2: For connecting to the PostgreSQL database.

Usage:
1. Ensure the `POSTGRES_PASSWORD` environment variable is set with the database password.
2. Run the script to clean and store the events data.

"""

import psycopg2
import os

def clean_events_data_from_postgres(conn):
    """
    Reads raw events data from the events_table, performs cleaning,
    and writes the cleaned data to the cleaned_events table in PostgreSQL.

    Args:
        conn: psycopg2 connection object.
    """
    try:
        cur = conn.cursor()

        # 1. Read raw events data from events_table
        # cur.execute("SELECT event_id, event_ts, user_id, content_id, event_type, playback_position, device FROM events_table")
        cur.execute("SELECT event_id, event_ts, user_id, content_id, event_type, playback_position, device FROM events_table order by event_ts ASC LIMIT 10000")
        raw_events = cur.fetchall()
        print(f"Read {len(raw_events)} rows from events_table.")

        cleaned_events_list = []
        valid_event_types = ["play", "pause", "stop", "complete", "seek"]

        # 2. Perform data cleaning and filtering
        for event in raw_events:
            event_id, event_ts, user_id, content_id, event_type, playback_position, device = event

            # Basic data type and presence checks (more robust checks can be added)
            if not all([event_id, isinstance(event_ts, (int, float)), user_id, content_id, event_type, isinstance(playback_position, (int, float)), device]):
                print(f"Warning: Skipping event with missing or invalid basic types: {event}")
                continue

            # Convert timestamps and playback position to integers if they are floats
            try:
                event_ts = int(event_ts)
                playback_position = int(playback_position)
            except (ValueError, TypeError):
                print(f"Warning: Skipping event with invalid numeric values: {event}")
                continue

            if event_type not in valid_event_types:
                print(f"Warning: Filtering out event with invalid event_type: {event}")
                continue

            if playback_position < 0:
                print(f"Warning: Filtering out event with negative playback_position: {event}")
                continue

            cleaned_events_list.append({
                "event_id": event_id,
                "event_ts": event_ts,
                "user_id": user_id,
                "content_id": content_id,
                "event_type": event_type,
                "playback_position": playback_position,
                "device": device
            })

        print(f"Cleaned and filtered {len(cleaned_events_list)} events.")

        # 3. Write the cleaned data to the cleaned_events table
        create_cleaned_events_table_sql = """
        CREATE TABLE IF NOT EXISTS cleaned_events (
            event_id VARCHAR(255) PRIMARY KEY,
            event_ts BIGINT,
            user_id VARCHAR(255),
            content_id VARCHAR(255),
            event_type VARCHAR(50),
            playback_position INTEGER,
            device VARCHAR(50)
        );
        """
        cur.execute(create_cleaned_events_table_sql)

        insert_cleaned_events_sql = """
        INSERT INTO cleaned_events (event_id, event_ts, user_id, content_id, event_type, playback_position, device)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING; -- Skip duplicates if they somehow exist
        """

        for event in cleaned_events_list:
            cur.execute(insert_cleaned_events_sql, (
                event["event_id"],
                event["event_ts"],
                event["user_id"],
                event["content_id"],
                event["event_type"],
                event["playback_position"],
                event["device"]
            ))

        conn.commit()
        print("Successfully wrote cleaned events to the cleaned_events table.")

    except psycopg2.Error as e:
        print(f"Error during events data cleaning and writing: {e}")
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
    db_password = os.environ.get('POSTGRES_PASSWORD')
    db_sslmode = 'require'

    conn = None
    try:
        if db_password is None:
            raise EnvironmentError("POSTGRES_PASSWORD environment variable not set.")

        db_connection_string = f"postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={db_sslmode}"
        conn = psycopg2.connect(db_connection_string)
        clean_events_data_from_postgres(conn)
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
    except EnvironmentError as e:
        print(f"Configuration Error: {e}")
    finally:
        if conn:
            conn.close()
            print("PostgreSQL connection closed.")
