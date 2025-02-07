import os
import sys
import argparse
from datetime import datetime
from api_client import ClinicalTrialsClient
from database import ClinicalTrialsDB

def backup_all_trials(max_trials=None, resume=False):
    """Backup all trials from clinicaltrials.gov."""
    client = ClinicalTrialsClient()
    db = ClinicalTrialsDB()

    try:
        total_stored = 0
        next_page_token = None

        # If resuming, get the last processed NCT ID and page token
        if resume:
            last_nct = db.get_last_processed_nct()
            if last_nct:
                print(f"\nResuming from NCT ID: {last_nct}")
                next_page_token = db.get_last_page_token()

        print("\nStarting full backup of ClinicalTrials.gov")
        print("=" * 50)

        while True:
            try:
                response = client.get_studies(page_token=next_page_token)

                if not response or 'studies' not in response:
                    print("Invalid response format")
                    break

                studies = response.get('studies', [])
                if not studies:
                    print("No more studies found.")
                    break

                print(f"\nProcessing batch of {len(studies)} studies...")

                # Bulk insert studies
                stored_count = db.bulk_insert_trials(studies)
                total_stored += stored_count

                print(f"Stored {stored_count} trials (Total: {total_stored})")

                if max_trials and total_stored >= max_trials:
                    print(f"\nReached maximum number of trials ({max_trials})")
                    break

                next_page_token = response.get('nextPageToken')
                if next_page_token:
                    db.update_last_page_token(next_page_token)
                else:
                    print("\nNo more pages available.")
                    break

            except Exception as e:
                print(f"Error fetching studies: {str(e)}")
                db.update_last_page_token(next_page_token)
                break

        return total_stored

    finally:
        db.close()

def update_trials():
    """Update trials that have changed since last update."""
    client = ClinicalTrialsClient()
    db = ClinicalTrialsDB()

    try:
        last_update = db.get_last_update_time()
        total_updated = 0
        next_page_token = None

        print(f"\nChecking for updates since {last_update}")
        print("=" * 50)

        while True:
            try:
                # Use the API fields parameter to get only what we need
                response = client.get_studies(page_token=next_page_token)

                if not response or 'studies' not in response:
                    break

                studies = response.get('studies', [])
                if not studies:
                    break

                print(f"\nProcessing {len(studies)} updated studies...")

                # Use bulk insert with ON CONFLICT DO UPDATE
                stored_count = db.bulk_insert_trials(studies)
                total_updated += stored_count

                print(f"Updated {stored_count} trials (Total: {total_updated})")

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break

            except Exception as e:
                print(f"Error fetching updates: {str(e)}")
                break

        return total_updated

    finally:
        db.close()

def main():
    parser = argparse.ArgumentParser(
        description='Backup and update clinical trials from ClinicalTrials.gov'
    )
    parser.add_argument('--max-trials',
                       type=int,
                       help='Maximum number of trials to fetch')
    parser.add_argument('--resume',
                       action='store_true',
                       help='Resume from last processed trial')
    parser.add_argument('--update-only',
                       action='store_true',
                       help='Only fetch trials updated since last backup')

    args = parser.parse_args()

    print("\nClinical Trials Backup System")
    print("=" * 50)
    start_time = datetime.now()

    if args.update_only:
        total_stored = update_trials()
    else:
        total_stored = backup_all_trials(
            max_trials=args.max_trials,
            resume=args.resume
        )

    end_time = datetime.now()
    duration = end_time - start_time

    print("\nBackup Summary")
    print("=" * 50)
    print(f"Total trials processed: {total_stored}")
    print(f"Time taken: {duration}")

if __name__ == "__main__":
    main()