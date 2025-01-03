import streamlit as st
from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv
from supabase import create_client
import requests
import time
import logging
from streamlit.runtime.scriptrunner import add_script_run_ctx
import threading

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

def fetch_and_store_fixtures(date):
    """Fetch fixtures from API and store them"""
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    
    querystring = {"date": str(date)}
    headers = {
        "x-rapidapi-key": os.getenv('RAPIDAPI_KEY'),
        "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
    }
    
    try:
        logging.info(f"Fetching fixtures for {date}")
        fixtures_response = requests.get(url, headers=headers, params=querystring)
        
        if fixtures_response.status_code != 200:
            raise Exception(f"API request failed with status {fixtures_response.status_code}")
        
        fixtures_data = fixtures_response.json()
        stored_count = 0
        
        # Store each fixture in Supabase
        for fixture in fixtures_data['response']:
            fixture_record = {
                'date': str(date),
                'fixture_id': fixture['fixture']['id'],
                'fixture_data': fixture,
                'created_at': datetime.utcnow().isoformat()
            }
            
            # Upsert the fixture
            supabase.table('fixtures').upsert(
                fixture_record,
                on_conflict='fixture_id'
            ).execute()
            stored_count += 1
        
        logging.info(f"Stored {stored_count} fixtures for {date}")
        return stored_count
            
    except Exception as e:
        logging.error(f"Error storing fixtures for {date}: {str(e)}")
        return 0

def scheduled_task():
    """Task to fetch fixtures for current day and next two days"""
    while True:
        now = datetime.now(pytz.UTC)
        
        # Check if it's 00:01 UTC
        if now.hour == 0 and now.minute == 1:
            logging.info("Starting scheduled fixtures fetch")
            
            try:
                current_date = now.date()
                
                # Process current day and next two days
                for i in range(3):
                    target_date = current_date + timedelta(days=i)
                    stored_count = fetch_and_store_fixtures(target_date)
                    time.sleep(5)  # Respect API rate limits
                
                logging.info("Scheduled task completed successfully")
                
            except Exception as e:
                logging.error(f"Error in scheduled task: {str(e)}")
            
            # Sleep until next minute to avoid multiple executions
            time.sleep(60)
        else:
            # Check every minute
            time.sleep(60)

def start_scheduler():
    """Start the scheduler in a separate thread"""
    if 'scheduler_started' not in st.session_state:
        scheduler_thread = threading.Thread(target=scheduled_task, daemon=True)
        add_script_run_ctx(scheduler_thread)
        scheduler_thread.start()
        st.session_state.scheduler_started = True
        logging.info("Scheduler started")

def main():
    st.title("⚽ Football Data Manager")
    
    # Start the scheduler
    start_scheduler()
    
    # Display status
    st.subheader("Service Status")
    st.info("Scheduler is running. Fixtures will be fetched daily at 00:01 UTC for the current day and next two days.")
    
    # Manual fetch section
    st.subheader("Manual Fetch")
    if st.button("Fetch Fixtures Now"):
        current_date = datetime.now(pytz.UTC).date()
        total_stored = 0
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i in range(3):
            target_date = current_date + timedelta(days=i)
            status_text.text(f"Fetching fixtures for {target_date}...")
            stored_count = fetch_and_store_fixtures(target_date)
            total_stored += stored_count
            progress_bar.progress((i + 1) / 3)
            time.sleep(5)  # Respect API rate limits
        
        st.success(f"Successfully stored {total_stored} fixtures")
    
    # Display current data stats
    st.subheader("Current Data Statistics")
    
    # Get dates to check
    current_date = datetime.now(pytz.UTC).date()
    dates_to_check = [current_date + timedelta(days=i) for i in range(3)]
    
    # Create columns for each date
    cols = st.columns(3)
    
    for i, date in enumerate(dates_to_check):
        with cols[i]:
            fixtures_count = len(
                supabase.table('fixtures')
                .select('fixture_id')
                .eq('date', str(date))
                .execute()
                .data
            )
            st.metric(f"Fixtures {date}", fixtures_count)

if __name__ == "__main__":
    main()