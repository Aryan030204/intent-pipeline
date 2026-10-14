"""Replaced version of pipeline/db.py update_pipeline_metadata_timestamp (kept for reference, never imported).

It ran CREATE TABLE IF NOT EXISTS pipeline_metadata before every watermark update. The live version upserts
first and creates the table only if MySQL says it does not exist (error 1146).
"""

def update_pipeline_metadata_timestamp(cursor, connection, key_name: str, new_timestamp: datetime):
    ensure_pipeline_metadata_table(cursor, connection)
    cursor.execute(
        """
        INSERT INTO pipeline_metadata (key_name, key_value)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE key_value = VALUES(key_value);
    """,
        (key_name, new_timestamp),
    )
    connection.commit()
