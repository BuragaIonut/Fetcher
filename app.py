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
import json

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

def load_major_leagues():
    """Load major leagues from JSON file"""
    try:
        with open("major_leagues.json", "r") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading major leagues: {str(e)}")
        return []

def fetch_and_store_fixtures(date):
    """Fetch fixtures from API and store them in the new structure"""
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
        
        # Store each fixture in Supabase with the new structure
        for fixture in fixtures_data['response']:
            fixture_record = {
                'fixture_id': fixture['fixture']['id'],
                'home_team_id': fixture['teams']['home']['id'],
                'home_team_name': fixture['teams']['home']['name'],
                'home_team_logo': fixture['teams']['home']['logo'],
                'away_team_id': fixture['teams']['away']['id'],
                'away_team_name': fixture['teams']['away']['name'],
                'away_team_logo': fixture['teams']['away']['logo'],
                'league_id': fixture['league']['id'],
                'league_name': fixture['league']['name'],
                'league_logo': fixture['league']['logo'],
                'league_flag': fixture['league'].get('flag'),
                'league_country': fixture['league']['country'],
                'fixture_date': fixture['fixture']['date'],
                'venue_id': fixture['fixture']['venue']['id'],
                'venue_city': fixture['fixture']['venue']['city'],
                'venue_name': fixture['fixture']['venue']['name'],
                'raw_data': fixture,
                'created_at': datetime.utcnow().isoformat()
            }
            
            # Upsert the fixture
            supabase.table('football_fixtures').upsert(
                fixture_record,
                on_conflict='fixture_id'
            ).execute()
            stored_count += 1
        
        logging.info(f"Stored {stored_count} fixtures for {date}")
        return stored_count
            
    except Exception as e:
        logging.error(f"Error storing fixtures for {date}: {str(e)}")
        return 0

def get_fixtures_stats(date):
    """Get statistics for fixtures on a specific date"""
    try:
        # Get total fixtures
        total_fixtures = supabase.table('football_fixtures') \
            .select('fixture_id') \
            .gte('fixture_date', f"{date}T00:00:00Z") \
            .lt('fixture_date', f"{date + timedelta(days=1)}T00:00:00Z") \
            .execute()
        
        # Get major league fixtures
        major_leagues = load_major_leagues()
        major_league_ids = [league['id'] for league in major_leagues]
        
        major_fixtures = supabase.table('football_fixtures') \
            .select('fixture_id') \
            .gte('fixture_date', f"{date}T00:00:00Z") \
            .lt('fixture_date', f"{date + timedelta(days=1)}T00:00:00Z") \
            .in_('league_id', major_league_ids) \
            .execute()
        
        return {
            'total': len(total_fixtures.data),
            'major': len(major_fixtures.data)
        }
    except Exception as e:
        logging.error(f"Error getting fixtures stats: {str(e)}")
        return {'total': 0, 'major': 0}

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

def main():
    st.title("⚽ Football Data Manager")
    
    # # Start the scheduler
    # if 'scheduler_started' not in st.session_state:
    #     scheduler_thread = threading.Thread(target=scheduled_task, daemon=True)
    #     add_script_run_ctx(scheduler_thread)
    #     scheduler_thread.start()
    #     st.session_state.scheduler_started = True
    #     logging.info("Scheduler started")
    
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
            stats = get_fixtures_stats(date)
            st.metric(f"Date: {date}", f"Total: {stats['total']}")
            st.metric("Major Leagues", stats['major'])

if __name__ == "__main__":
    main()