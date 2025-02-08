import os
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import Dict, List, Optional

class ClinicalTrialsDB:
    def __init__(self):
        self.conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        self.create_tables()

    def create_tables(self):
        """Create the necessary database tables if they don't exist."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clinical_trials (
                    id SERIAL PRIMARY KEY,
                    nct_id VARCHAR(255) UNIQUE NOT NULL,
                    data JSONB NOT NULL,
                    conditions JSONB NOT NULL,
                    interventions JSONB NOT NULL,
                    has_results BOOLEAN GENERATED ALWAYS AS ((data->>'hasResults')::boolean) STORED,
                    overall_status VARCHAR(50) GENERATED ALWAYS AS ((data->'protocolSection'->'statusModule'->>'overallStatus')::text) STORED,
                    phase VARCHAR(50) GENERATED ALWAYS AS (
                        CASE 
                            WHEN jsonb_array_length(data->'protocolSection'->'designModule'->'phases') > 0 
                            THEN (data->'protocolSection'->'designModule'->'phases'->0)::text 
                            ELSE NULL 
                        END
                    ) STORED,
                    brief_title TEXT GENERATED ALWAYS AS ((data->'protocolSection'->'identificationModule'->>'briefTitle')::text) STORED,
                    created_at TIMESTAMP DEFAULT NOW(),
                    last_updated_at TIMESTAMP DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_gin_data ON clinical_trials USING GIN (data jsonb_path_ops);
                CREATE INDEX IF NOT EXISTS idx_conditions ON clinical_trials USING GIN (conditions);
                CREATE INDEX IF NOT EXISTS idx_interventions ON clinical_trials USING GIN (interventions);
                CREATE INDEX IF NOT EXISTS idx_has_results ON clinical_trials (has_results);
                CREATE INDEX IF NOT EXISTS idx_overall_status ON clinical_trials (overall_status);
                CREATE INDEX IF NOT EXISTS idx_phase ON clinical_trials (phase);

                CREATE TABLE IF NOT EXISTS backup_state (
                    id SERIAL PRIMARY KEY,
                    last_page_token TEXT,
                    last_update_time TIMESTAMP,
                    last_processed_nct VARCHAR(255)
                );

                INSERT INTO backup_state (last_page_token, last_update_time)
                SELECT NULL, NOW()
                WHERE NOT EXISTS (SELECT 1 FROM backup_state);
            """)
            self.conn.commit()

    def clean_text(self, text: str) -> str:
        """Clean text by removing formatting artifacts and standardizing spacing."""
        if not text:
            return ''

        # Remove any surrounding quotes and whitespace
        text = text.strip('"\' ')

        # Normalize internal whitespace
        return ' '.join(text.split())

    def extract_conditions(self, data: Dict) -> List[str]:
        """Extract and clean conditions from trial data."""
        try:
            conditions = data.get('protocolSection', {}).get('conditionsModule', {}).get('conditions', [])
            cleaned_conditions = []
            for condition in conditions:
                if condition:
                    cleaned = self.clean_text(condition)
                    if cleaned:
                        cleaned_conditions.append(cleaned)
            return cleaned_conditions
        except Exception:
            return []

    def extract_interventions(self, data: Dict) -> List[str]:
        """Extract and clean interventions from trial data."""
        try:
            interventions = data.get('protocolSection', {}).get('armsInterventionsModule', {}).get('interventions', [])
            cleaned_interventions = []
            for intervention in interventions:
                name = intervention.get('name', '')
                if name:
                    cleaned = self.clean_text(name)
                    if cleaned:
                        cleaned_interventions.append(cleaned)
            return cleaned_interventions
        except Exception:
            return []

    def bulk_insert_trials(self, studies: List[Dict]) -> int:
        """Bulk insert or update trials in the database."""
        if not studies:
            return 0

        with self.conn.cursor() as cur:
            values = []
            for study in studies:
                protocol_section = study.get('protocolSection', {})
                identification = protocol_section.get('identificationModule', {})
                nct_id = identification.get('nctId')

                if not nct_id:
                    continue

                # Extract and clean arrays
                conditions = self.extract_conditions(study)
                interventions = self.extract_interventions(study)

                # Convert to JSON arrays with proper formatting and ensure UTF-8 encoding
                conditions_json = json.dumps(conditions, ensure_ascii=False)
                interventions_json = json.dumps(interventions, ensure_ascii=False)

                values.append((
                    nct_id,
                    json.dumps(study),
                    conditions_json,
                    interventions_json
                ))

            if not values:
                return 0

            execute_values(cur, """
                INSERT INTO clinical_trials (nct_id, data, conditions, interventions)
                VALUES %s
                ON CONFLICT (nct_id) DO UPDATE 
                SET data = EXCLUDED.data,
                    conditions = EXCLUDED.conditions,
                    interventions = EXCLUDED.interventions,
                    last_updated_at = NOW()
                """, values)

            self.conn.commit()
            self.update_last_processed_nct(values[-1][0])

            return len(values)

    def get_last_update_time(self) -> datetime:
        """Get the timestamp of the last update."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT last_update_time FROM backup_state LIMIT 1")
            result = cur.fetchone()
            return result[0] if result else datetime.min

    def get_last_processed_nct(self) -> Optional[str]:
        """Get the NCT ID of the last processed trial."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT last_processed_nct FROM backup_state LIMIT 1")
            result = cur.fetchone()
            return result[0] if result else None

    def update_last_processed_nct(self, nct_id: str):
        """Update the last processed NCT ID."""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE backup_state 
                SET last_processed_nct = %s,
                    last_update_time = NOW()
            """, (nct_id,))
            self.conn.commit()

    def get_last_page_token(self) -> Optional[str]:
        """Get the last page token."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT last_page_token FROM backup_state LIMIT 1")
            result = cur.fetchone()
            return result[0] if result else None

    def update_last_page_token(self, token: str):
        """Update the last page token."""
        with self.conn.cursor() as cur:
            cur.execute("UPDATE backup_state SET last_page_token = %s", (token,))
            self.conn.commit()

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def trial_exists(self, nct_id: str) -> bool:
        """Check if a trial already exists in the database."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM clinical_trials WHERE nct_id = %s)", (nct_id,))
            return cur.fetchone()[0]