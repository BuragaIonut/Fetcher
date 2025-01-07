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

def calculate_interval_averages(minute_data, games_played):
    """
    Calculate averages for first and second half from minute data
    """
    if games_played == 0:
        return 0, 0
        
    first_half = sum(
        minute_data[interval]["total"] or 0 
        for interval in ["0-15", "16-30", "31-45"]
    ) / games_played
    
    second_half = sum(
        minute_data[interval]["total"] or 0 
        for interval in ["46-60", "61-75", "76-90"]
    ) / games_played
    
    return round(first_half, 2), round(second_half, 2)

def process_team_stats(team_data):
    """
    Process all relevant statistics for a team
    """
    stats = {}
    
    # Get games played
    home_games = team_data['league']['fixtures']['played']['home']
    away_games = team_data['league']['fixtures']['played']['away']
    
    # Process goals
    goals_for = team_data['league']['goals']['for']['minute']
    goals_against = team_data['league']['goals']['against']['minute']
    
    # Calculate scoring averages
    if home_games > 0:
        home_first_half, home_second_half = calculate_interval_averages(goals_for, home_games)
        stats['scored_home_first_half_average'] = home_first_half
        stats['scored_home_second_half_average'] = home_second_half
        
        home_conc_first, home_conc_second = calculate_interval_averages(goals_against, home_games)
        stats['conceded_home_first_half_average'] = home_conc_first
        stats['conceded_home_second_half_average'] = home_conc_second
    
    if away_games > 0:
        away_first_half, away_second_half = calculate_interval_averages(goals_for, away_games)
        stats['scored_away_first_half_average'] = away_first_half
        stats['scored_away_second_half_average'] = away_second_half
        
        away_conc_first, away_conc_second = calculate_interval_averages(goals_against, away_games)
        stats['conceded_away_first_half_average'] = away_conc_first
        stats['conceded_away_second_half_average'] = away_conc_second
    
    # Process yellow cards
    yellow_cards = team_data['league']['cards']['yellow']
    total_games = home_games + away_games
    if total_games > 0:
        cards_first_half, cards_second_half = calculate_interval_averages(yellow_cards, total_games)
        stats['yellow_cards_first_half_average'] = cards_first_half
        stats['yellow_cards_second_half_average'] = cards_second_half
    
    return stats

def fetch_and_store_predictions(fixture_id):
    """Fetch and store predictions with additional statistics"""
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
        
        # Store main prediction data
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
        
        # Store main predictions
        supabase.table('football_predictions').upsert(
            prediction_record,
            on_conflict='fixture_id'
        ).execute()
        
        # Process stats for both teams
        home_stats = process_team_stats(pred['teams']['home'])
        away_stats = process_team_stats(pred['teams']['away'])
        
        # Prepare stats record
        stats_record = {
            'fixture_id': fixture_id,
            # Home team stats
            'home_team_yellow_cards_first_half_average': home_stats.get('yellow_cards_first_half_average'),
            'home_team_yellow_cards_second_half_average': home_stats.get('yellow_cards_second_half_average'),
            'home_team_scored_home_first_half_average': home_stats.get('scored_home_first_half_average'),
            'home_team_scored_home_second_half_average': home_stats.get('scored_home_second_half_average'),
            'home_team_scored_away_first_half_average': home_stats.get('scored_away_first_half_average'),
            'home_team_scored_away_second_half_average': home_stats.get('scored_away_second_half_average'),
            'home_team_conceded_home_first_half_average': home_stats.get('conceded_home_first_half_average'),
            'home_team_conceded_home_second_half_average': home_stats.get('conceded_home_second_half_average'),
            'home_team_conceded_away_first_half_average': home_stats.get('conceded_away_first_half_average'),
            'home_team_conceded_away_second_half_average': home_stats.get('conceded_away_second_half_average'),
            # Away team stats
            'away_team_yellow_cards_first_half_average': away_stats.get('yellow_cards_first_half_average'),
            'away_team_yellow_cards_second_half_average': away_stats.get('yellow_cards_second_half_average'),
            'away_team_scored_home_first_half_average': away_stats.get('scored_home_first_half_average'),
            'away_team_scored_home_second_half_average': away_stats.get('scored_home_second_half_average'),
            'away_team_scored_away_first_half_average': away_stats.get('scored_away_first_half_average'),
            'away_team_scored_away_second_half_average': away_stats.get('scored_away_second_half_average'),
            'away_team_conceded_home_first_half_average': away_stats.get('conceded_home_first_half_average'),
            'away_team_conceded_home_second_half_average': away_stats.get('conceded_home_second_half_average'),
            'away_team_conceded_away_first_half_average': away_stats.get('conceded_away_first_half_average'),
            'away_team_conceded_away_second_half_average': away_stats.get('conceded_away_second_half_average'),
        }
        
        # Store the stats
        supabase.table('football_predictions_stats').upsert(
            stats_record,
            on_conflict='fixture_id'
        ).execute()
        
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
    
    # Add date picker
    selected_date = st.date_input(
        "Select date for data fetch",
        value=datetime.now(pytz.UTC).date(),
        min_value=datetime.now(pytz.UTC).date() - timedelta(days=30),
        max_value=datetime.now(pytz.UTC).date() + timedelta(days=30)
    )
    
    # Manual fetch section
    st.subheader("Manual Fetch")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Fetch Fixtures Now"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.text(f"Fetching fixtures for {selected_date}...")
            stored_count = fetch_and_store_fixtures(selected_date)
            progress_bar.progress(1.0)
            
            st.success(f"Successfully stored {stored_count} fixtures")
        
        # Add button to delete fixtures
        if st.button("Delete Fixtures for Selected Date"):
            # Logic to delete fixtures
            supabase.table('football_fixtures').delete().eq('fixture_date', f"{selected_date}T00:00:00Z").execute()
            st.success(f"Successfully deleted fixtures for {selected_date}")

    with col2:
        if st.button("Fetch Predictions Now"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.text(f"Fetching predictions for {selected_date}...")
            
            # Get fixtures for major leagues
            fixture_ids = get_major_league_fixtures(selected_date)
            total_predictions = 0
            
            for idx, fixture_id in enumerate(fixture_ids):
                if fetch_and_store_predictions(fixture_id):
                    total_predictions += 1
                progress_bar.progress((idx + 1) / len(fixture_ids) if fixture_ids else 1.0)
                time.sleep(1)  # Respect API rate limits
            
            st.success(f"Successfully stored {total_predictions} predictions")
        
        # Add button to delete predictions
        if st.button("Delete Predictions for Selected Date"):
            # Logic to delete predictions
            supabase.table('football_predictions').delete().in_('fixture_id', get_major_league_fixtures(selected_date)).execute()
            st.success(f"Successfully deleted predictions for {selected_date}")

    # Display current data stats
    st.subheader("Current Data Statistics")
    
    stats = get_fixtures_stats(selected_date)
    st.metric(f"Date: {selected_date}", f"Total: {stats['total']}")
    st.metric("Major Leagues", stats['major'])

if __name__ == "__main__":
    main()