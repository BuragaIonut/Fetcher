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

def fetch_and_store_predictions(fixture_id):
    """Fetch predictions from API and store them in the database"""
    url = "https://api-football-v1.p.rapidapi.com/v3/predictions"
    querystring = {"fixture": fixture_id}
    headers = {
        "x-rapidapi-key": os.getenv('RAPIDAPI_KEY'),
        "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
    }
    
    try:
        logging.info(f"Fetching predictions for fixture {fixture_id}")
        response = requests.get(url, headers=headers, params=querystring)
        
        if response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}")
        
        prediction_data = response.json()
        
        if not prediction_data['response']:
            logging.warning(f"No prediction data found for fixture {fixture_id}")
            return False
            
        pred = prediction_data['response'][0]
        
        prediction_record = {
            'fixture_id': fixture_id,
            'winner_team_name': pred['predictions']['winner']['name'] if pred['predictions']['winner'] else None,
            'winner_comment': pred['predictions']['winner']['comment'] if pred['predictions']['winner'] else None,
            'win_or_draw': pred['predictions']['win_or_draw'],
            'under_over': pred['predictions']['under_over'],
            'goals_home': pred['predictions']['goals']['home'],
            'goals_away': pred['predictions']['goals']['away'],
            'advice': pred['predictions']['advice'],
            'percent_home': pred['predictions']['percent']['home'],
            'percent_draw': pred['predictions']['percent']['draw'],
            'percent_away': pred['predictions']['percent']['away'],
            'comp_form_home': pred['comparison']['form']['home'],
            'comp_form_away': pred['comparison']['form']['away'],
            'comp_att_home': pred['comparison']['att']['home'],
            'comp_att_away': pred['comparison']['att']['away'],
            'comp_def_home': pred['comparison']['def']['home'],
            'comp_def_away': pred['comparison']['def']['away'],
            'comp_poisson_home': pred['comparison']['poisson_distribution']['home'],
            'comp_poisson_away': pred['comparison']['poisson_distribution']['away'],
            'comp_h2h_home': pred['comparison']['h2h']['home'],
            'comp_h2h_away': pred['comparison']['h2h']['away'],
            'comp_goals_home': pred['comparison']['goals']['home'],
            'comp_goals_away': pred['comparison']['goals']['away'],
            'comp_total_home': pred['comparison']['total']['home'],
            'comp_total_away': pred['comparison']['total']['away']
        }
        
        # Get the prediction_id from the insert response
        prediction_response = supabase.table('football_predictions') \
            .insert(prediction_record) \
            .execute()
        
        prediction_id = prediction_response.data[0]['id']
        
        # Store goals distribution for both teams
        for team_side in ['home', 'away']:
            # Store goals for
            goals_for = pred['teams'][team_side]['league']['goals']['for']['minute']
            goals_for_record = {
                'prediction_id': prediction_id,
                'team_side': team_side,
                'goal_type': 'for',
                'interval_0_15': goals_for['0-15']['total'] or 0,
                'interval_0_15_percentage': goals_for['0-15']['percentage'],
                'interval_16_30': goals_for['16-30']['total'] or 0,
                'interval_16_30_percentage': goals_for['16-30']['percentage'],
                'interval_31_45': goals_for['31-45']['total'] or 0,
                'interval_31_45_percentage': goals_for['31-45']['percentage'],
                'interval_46_60': goals_for['46-60']['total'] or 0,
                'interval_46_60_percentage': goals_for['46-60']['percentage'],
                'interval_61_75': goals_for['61-75']['total'] or 0,
                'interval_61_75_percentage': goals_for['61-75']['percentage'],
                'interval_76_90': goals_for['76-90']['total'] or 0,
                'interval_76_90_percentage': goals_for['76-90']['percentage'],
                'interval_91_105': goals_for['91-105']['total'] or 0,
                'interval_91_105_percentage': goals_for['91-105']['percentage'],
                'interval_106_120': goals_for['106-120']['total'] or 0,
                'interval_106_120_percentage': goals_for['106-120']['percentage']
            }
            
            # Store goals against
            goals_against = pred['teams'][team_side]['league']['goals']['against']['minute']
            goals_against_record = {
                'prediction_id': prediction_id,
                'team_side': team_side,
                'goal_type': 'against',
                'interval_0_15': goals_against['0-15']['total'] or 0,
                'interval_0_15_percentage': goals_against['0-15']['percentage'],
                'interval_16_30': goals_against['16-30']['total'] or 0,
                'interval_16_30_percentage': goals_against['16-30']['percentage'],
                'interval_31_45': goals_against['31-45']['total'] or 0,
                'interval_31_45_percentage': goals_against['31-45']['percentage'],
                'interval_46_60': goals_against['46-60']['total'] or 0,
                'interval_46_60_percentage': goals_against['46-60']['percentage'],
                'interval_61_75': goals_against['61-75']['total'] or 0,
                'interval_61_75_percentage': goals_against['61-75']['percentage'],
                'interval_76_90': goals_against['76-90']['total'] or 0,
                'interval_76_90_percentage': goals_against['76-90']['percentage'],
                'interval_91_105': goals_against['91-105']['total'] or 0,
                'interval_91_105_percentage': goals_against['91-105']['percentage'],
                'interval_106_120': goals_against['106-120']['total'] or 0,
                'interval_106_120_percentage': goals_against['106-120']['percentage']
            }
            
            # Insert goals records
            supabase.table('football_predictions_goals').insert([goals_for_record, goals_against_record]).execute()
            
            # Store cards distribution
            for card_type in ['yellow', 'red']:
                cards = pred['teams'][team_side]['league']['cards'][card_type]
                cards_record = {
                    'prediction_id': prediction_id,
                    'team_side': team_side,
                    'card_type': card_type,
                    'interval_0_15': cards['0-15']['total'] or 0,
                    'interval_0_15_percentage': cards['0-15']['percentage'],
                    'interval_16_30': cards['16-30']['total'] or 0,
                    'interval_16_30_percentage': cards['16-30']['percentage'],
                    'interval_31_45': cards['31-45']['total'] or 0,
                    'interval_31_45_percentage': cards['31-45']['percentage'],
                    'interval_46_60': cards['46-60']['total'] or 0,
                    'interval_46_60_percentage': cards['46-60']['percentage'],
                    'interval_61_75': cards['61-75']['total'] or 0,
                    'interval_61_75_percentage': cards['61-75']['percentage'],
                    'interval_76_90': cards['76-90']['total'] or 0,
                    'interval_76_90_percentage': cards['76-90']['percentage'],
                    'interval_91_105': cards['91-105']['total'] or 0,
                    'interval_91_105_percentage': cards['91-105']['percentage'],
                    'interval_106_120': cards['106-120']['total'] or 0,
                    'interval_106_120_percentage': cards['106-120']['percentage']
                }
                
                # Insert cards record
                supabase.table('football_predictions_cards').insert(cards_record).execute()
        
        logging.info(f"Stored predictions and statistics for fixture {fixture_id}")
        return True
        
    except Exception as e:
        logging.error(f"Error storing predictions for fixture {fixture_id}: {str(e)}")
        return False

def get_major_league_fixtures(date):
    """Get fixtures from major leagues for a specific date"""
    try:
        major_leagues = load_major_leagues()
        major_league_ids = [league['id'] for league in major_leagues]
        
        fixtures = supabase.table('football_fixtures') \
            .select('fixture_id') \
            .gte('fixture_date', f"{date}T00:00:00Z") \
            .lt('fixture_date', f"{date + timedelta(days=1)}T00:00:00Z") \
            .in_('league_id', major_league_ids) \
            .execute()
            
        return [fixture['fixture_id'] for fixture in fixtures.data]
    except Exception as e:
        logging.error(f"Error getting major league fixtures: {str(e)}")
        return []

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
    st.title("⚽ Football Data Manager ⚽")
    
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
    col1, col2 = st.columns(2)
    
    with col1:
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
    
    with col2:
        if st.button("Fetch Predictions Now"):
            current_date = datetime.now(pytz.UTC).date()
            total_predictions = 0
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i in range(3):
                target_date = current_date + timedelta(days=i)
                status_text.text(f"Fetching predictions for {target_date}...")
                
                # Get fixtures for major leagues
                fixture_ids = get_major_league_fixtures(target_date)
                predictions_stored = 0
                
                for idx, fixture_id in enumerate(fixture_ids):
                    if fetch_and_store_predictions(fixture_id):
                        predictions_stored += 1
                    progress_bar.progress((i * len(fixture_ids) + idx + 1) / (len(fixture_ids) * 3))
                    time.sleep(1)  # Respect API rate limits
                
                total_predictions += predictions_stored
                logging.info(f"Stored {predictions_stored} predictions for {target_date}")
            
            st.success(f"Successfully stored {total_predictions} predictions")
        
    
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