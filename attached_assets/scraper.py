import os
import sys
import json
import psycopg2
from typing import Dict, Optional, List
from dotenv import load_dotenv
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
import argparse
from datetime import datetime

# Load environment variables
load_dotenv()


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
                    has_results BOOLEAN GENERATED ALWAYS AS ((data->>'hasResults')::boolean) STORED,
                    overall_status VARCHAR(50) GENERATED ALWAYS AS ((data->'protocolSection'->'statusModule'->>'overallStatus')::text) STORED,
                    phase VARCHAR(50) GENERATED ALWAYS AS (
                        CASE 
                            WHEN jsonb_array_length(data->'protocolSection'->'designModule'->'phases') > 0 
                            THEN (data->'protocolSection'->'designModule'->'phases'->0)::text 
                            ELSE NULL 
                        END
                    ) STORED,
                    search_terms TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    last_updated_at TIMESTAMP DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_gin_data ON clinical_trials USING GIN (data jsonb_path_ops);
                CREATE INDEX IF NOT EXISTS idx_has_results ON clinical_trials (has_results);
                CREATE INDEX IF NOT EXISTS idx_overall_status ON clinical_trials (overall_status);
                CREATE INDEX IF NOT EXISTS idx_phase ON clinical_trials (phase);
            """)
            self.conn.commit()

    def insert_trial(self, nct_id: str, data: Dict, search_terms: str) -> int:
        """Insert or update a trial in the database."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO clinical_trials (nct_id, data, search_terms, last_updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (nct_id) DO UPDATE 
                SET data = EXCLUDED.data,
                    search_terms = CASE 
                        WHEN clinical_trials.search_terms IS NULL THEN EXCLUDED.search_terms
                        WHEN position(EXCLUDED.search_terms in clinical_trials.search_terms) > 0 THEN clinical_trials.search_terms
                        ELSE clinical_trials.search_terms || '; ' || EXCLUDED.search_terms
                    END,
                    last_updated_at = NOW()
                RETURNING id;
            """, (nct_id, json.dumps(data), search_terms))
            self.conn.commit()
            return cur.fetchone()[0]

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()


class ClinicalTrialsClient:

    def __init__(self):
        self.base_url = "https://clinicaltrials.gov/api/v2/studies"
        self.headers = {
            "Accept": "application/json",
            "User-Agent": "ClinicalTrialsScraper/1.0"
        }

    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=4, max=10))
    def search_studies(self,
                       condition: Optional[str] = None,
                       intervention: Optional[str] = None,
                       other_terms: Optional[str] = None,
                       page_token: Optional[str] = None,
                       results_only: bool = False) -> Dict:
        """Search for studies using the API's search endpoint."""
        params = {
            'format': 'json',
            'pageSize': 100,
        }

        # Build the search query
        queries = []
        if condition:
            params['query.cond'] = f'"{condition}"'
        if intervention:
            params['query.intr'] = f'"{intervention}"'
        if other_terms:
            params['query.term'] = f'"{other_terms}"'
        if results_only:
            params['query.term'] = ('query.term' in params and f'{params["query.term"]} AND AREA[HasResults] true') or 'AREA[HasResults] true'

        if page_token:
            params['pageToken'] = page_token

        print(f"\nRequesting URL: {self.base_url}")
        print(f"Query parameters: {params}")

        try:
            response = requests.get(self.base_url,
                                    params=params,
                                    headers=self.headers,
                                    timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                print(f"Response content: {e.response.text}")
            raise


def scrape_trials(condition: Optional[str] = None,
                   intervention: Optional[str] = None,
                   other_terms: Optional[str] = None,
                   max_trials: Optional[int] = None,
                   results_only: bool = False):
    """Main function to scrape and store trials."""
    if not any([condition, intervention, other_terms]):
        raise ValueError(
            "At least one of condition, intervention, or other_terms must be specified")

    client = ClinicalTrialsClient()
    db = ClinicalTrialsDB()

    try:
        total_stored = 0
        next_page_token = None
        search_terms = []
        if condition:
            search_terms.append(f"condition:{condition}")
        if intervention:
            search_terms.append(f"intervention:{intervention}")
        if other_terms:
            search_terms.append(f"other_terms:{other_terms}")
        if results_only:
            search_terms.append("has_results:true")

        search_terms_str = "; ".join(search_terms)

        print(f"\nStarting scraper with search terms: {search_terms_str}")
        print(
            f"Maximum trials to fetch: {max_trials if max_trials else 'unlimited'}"
        )

        while True:
            try:
                response = client.search_studies(condition=condition,
                                                 intervention=intervention,
                                                 other_terms=other_terms,
                                                 page_token=next_page_token,
                                                 results_only=results_only)

                if not response or 'studies' not in response:
                    print("Invalid response format")
                    break

                studies = response.get('studies', [])
                if not studies:
                    print("No more studies found.")
                    break

                print(f"\nProcessing batch of {len(studies)} studies...")

                for study in studies:
                    try:
                        protocol_section = study.get('protocolSection', {})
                        identification = protocol_section.get(
                            'identificationModule', {})
                        status_module = protocol_section.get(
                            'statusModule', {})

                        nct_id = identification.get('nctId')
                        if not nct_id:
                            print("Skipping study without NCT ID")
                            continue

                        has_results = study.get('hasResults', False)
                        status = status_module.get('overallStatus', 'UNKNOWN')

                        db.insert_trial(nct_id, study, search_terms_str)
                        total_stored += 1

                        print(
                            f"Stored trial {nct_id} - Status: {status} - Has Results: {has_results} (Total: {total_stored})"
                        )

                        if max_trials and total_stored >= max_trials:
                            print(
                                f"\nReached maximum number of trials ({max_trials})"
                            )
                            return total_stored

                    except Exception as e:
                        print(f"Error processing study: {str(e)}")
                        continue

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    print("\nNo more pages available.")
                    break

            except Exception as e:
                print(f"Error fetching studies: {str(e)}")
                break

        print(f"\nScraping completed. Total trials stored: {total_stored}")
        return total_stored

    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description='Scrape clinical trials data from ClinicalTrials.gov')
    parser.add_argument('--condition', help='Medical condition to search for')
    parser.add_argument('--intervention',
                        help='Treatment/intervention to search for')
    parser.add_argument('--other-terms',
                        help='Additional search terms to filter trials')
    parser.add_argument('--max-trials',
                        type=int,
                        help='Maximum number of trials to fetch')
    parser.add_argument('--results-only',
                        action='store_true',
                        help='Only fetch trials with results')

    args = parser.parse_args()

    if not any([args.condition, args.intervention, args.other_terms]):
        parser.error(
            "At least one of --condition, --intervention, or --other-terms must be specified")

    print("\nClinical Trials Scraper")
    print("=" * 50)
    start_time = datetime.now()

    total_stored = scrape_trials(condition=args.condition,
                                intervention=args.intervention,
                                other_terms=args.other_terms,
                                max_trials=args.max_trials,
                                results_only=args.results_only)

    end_time = datetime.now()
    duration = end_time - start_time

    print("\nScraping Summary")
    print("=" * 50)
    print(f"Total trials stored: {total_stored}")
    print(f"Time taken: {duration}")


if __name__ == "__main__":
    main()