import streamlit as st
from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv
from supabase import create_client

import time
import logging
import json
from langchain.prompts import PromptTemplate
from langchain_anthropic import ChatAnthropic
import asyncio
import aiohttp
from typing import List, Dict

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

async def store_fixture_async(fixture: Dict, supabase_client):
    """Async function to store a single fixture"""
    try:
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
            'ht_home_score': fixture['score']['halftime']['home'],
            'ht_away_score': fixture['score']['halftime']['away'],
            'ft_home_score': fixture['score']['fulltime']['home'],
            'ft_away_score': fixture['score']['fulltime']['away'],
            'created_at': datetime.utcnow().isoformat()
        }
        
        await supabase_client.table('football_fixtures').upsert(
            fixture_record,
            on_conflict='fixture_id'
        ).execute()
        return True
    except Exception as e:
        logging.error(f"Error storing fixture {fixture['fixture']['id']}: {str(e)}")
        return False

async def fetch_and_store_fixtures(date):
    """Fetch fixtures from API and store them asynchronously"""
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    
    querystring = {"date": str(date)}
    headers = {
        "x-rapidapi-key": os.getenv('RAPIDAPI_KEY'),
        "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
    }
    
    try:
        logging.info(f"Fetching fixtures for {date}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=querystring) as response:
                if response.status != 200:
                    raise Exception(f"API request failed with status {response.status}")
                
                fixtures_data = await response.json()
                
                # Create tasks for all fixtures
                tasks = [
                    store_fixture_async(fixture, supabase)
                    for fixture in fixtures_data['response']
                ]
                
                # Execute all tasks concurrently
                results = await asyncio.gather(*tasks)
                
                # Count successful stores
                stored_count = sum(1 for result in results if result)
                
                logging.info(f"Stored {stored_count} fixtures for {date}")
                return stored_count
                
    except Exception as e:
        logging.error(f"Error storing fixtures for {date}: {str(e)}")
        return 0

def calculate_interval_averages(minute_data, games_played):
    """
    Calculate averages for first and second half from minute data
    Return None if no valid data available
    """
    if games_played == 0:
        return None, None  # Return None if no games played
    
    # Check if we have any non-null data
    first_half_intervals = ["0-15", "16-30", "31-45"]
    second_half_intervals = ["46-60", "61-75", "76-90"]
    
    has_first_half_data = any(
        minute_data.get(interval, {}).get("total") is not None 
        for interval in first_half_intervals
    )
    
    has_second_half_data = any(
        minute_data.get(interval, {}).get("total") is not None 
        for interval in second_half_intervals
    )
    
    # Calculate averages only if we have data
    first_half = (sum(
        minute_data.get(interval, {}).get("total") or 0 
        for interval in first_half_intervals
    ) / games_played) if has_first_half_data else None
    
    second_half = (sum(
        minute_data.get(interval, {}).get("total") or 0 
        for interval in second_half_intervals
    ) / games_played) if has_second_half_data else None
    
    return (
        round(first_half, 2) if first_half is not None else None,
        round(second_half, 2) if second_half is not None else None
    )

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

async def store_prediction_async(prediction_data, fixture_id, supabase_client):
    """Async function to store prediction and stats"""
    try:
        pred = prediction_data['response'][0]
        
        # Prepare prediction record
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
        
        # Process stats
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
        
        # Execute both operations concurrently
        await asyncio.gather(
            supabase_client.table('football_predictions').upsert(
                prediction_record,
                on_conflict='fixture_id'
            ).execute(),
            supabase_client.table('football_predictions_stats').upsert(
                stats_record,
                on_conflict='fixture_id'
            ).execute()
        )
        
        return True
    except Exception as e:
        logging.error(f"Error storing prediction for fixture {fixture_id}: {str(e)}")
        return False

async def fetch_predictions_batch(fixture_ids: List[int]):
    """Fetch and store predictions for multiple fixtures concurrently"""
    async with aiohttp.ClientSession() as session:
        tasks = []
        api_key = os.getenv('RAPIDAPI_KEY')
        
        # Create tasks for all fixtures
        for fixture_id in fixture_ids:
            task = fetch_prediction_async(session, fixture_id, api_key)
            tasks.append(task)
        
        # Process results as they complete
        successful_predictions = 0
        failed_predictions = []
        
        for completed_task in asyncio.as_completed(tasks):
            fixture_id, result = await completed_task
            if result and result.get('response'):
                success = await store_prediction_async(result, fixture_id, supabase)
                if success:
                    successful_predictions += 1
                else:
                    failed_predictions.append(fixture_id)
            else:
                failed_predictions.append(fixture_id)
        
        return successful_predictions, failed_predictions

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

def get_common_timezones():
    return [
        'UTC',
        'Europe/Bucharest',
        'Europe/London',
        'Europe/Paris',
        'Europe/Madrid',
        'America/New_York',
        'Asia/Tokyo'
    ]

def get_major_fixtures_details(date, timezone='Europe/Bucharest'):
    try:
        major_leagues = load_major_leagues()
        major_league_ids = [league['id'] for league in major_leagues]
        
        fixtures = supabase.table('football_fixtures') \
            .select('*') \
            .gte('fixture_date', f"{date}T00:00:00Z") \
            .lt('fixture_date', f"{date + timedelta(days=1)}T00:00:00Z") \
            .in_('league_id', major_league_ids) \
            .execute()
            
        # Convert times to selected timezone
        for fixture in fixtures.data:
            utc_time = datetime.fromisoformat(fixture['fixture_date'].replace('Z', '+00:00'))
            local_time = utc_time.astimezone(pytz.timezone(timezone))
            fixture['local_time'] = local_time
            
        return fixtures.data
    except Exception as e:
        logging.error(f"Error getting major fixtures details: {str(e)}")
        return []

def get_fixture_predictions(fixture_id):
    """Get predictions and stats for a specific fixture"""
    try:
        # Get predictions
        predictions = supabase.table('football_predictions') \
            .select('*') \
            .eq('fixture_id', fixture_id) \
            .execute()
            
        # Get stats
        stats = supabase.table('football_predictions_stats') \
            .select('*') \
            .eq('fixture_id', fixture_id) \
            .execute()
            
        return {
            'predictions': predictions.data[0] if predictions.data else None,
            'stats': stats.data[0] if stats.data else None
        }
    except Exception as e:
        logging.error(f"Error getting fixture predictions: {str(e)}")
        return {'predictions': None, 'stats': None}

def get_teams_names(fixture_id):
    """Get the names of the teams for a specific fixture."""
    try:
        fixture = supabase.table('football_fixtures') \
            .select('home_team_name, away_team_name') \
            .eq('fixture_id', fixture_id) \
            .execute()
        
        if fixture.data:
            return {
                'home_team_name': fixture.data[0]['home_team_name'],
                'away_team_name': fixture.data[0]['away_team_name']
            }
        return None
    except Exception as e:
        logging.error(f"Error getting team names for fixture {fixture_id}: {str(e)}")
        return None

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
                    asyncio.run(fetch_and_store_fixtures(target_date))
                    time.sleep(5)  # Respect API rate limits
                
                logging.info("Scheduled task completed successfully")
                
            except Exception as e:
                logging.error(f"Error in scheduled task: {str(e)}")
            
            # Sleep until next minute to avoid multiple executions
            time.sleep(60)
        else:
            # Check every minute
            time.sleep(60)

def insert_match_predictions(fixture_id, model_response):
    """Insert match predictions into the database."""
    try:
        # Prepare the data for insertion
        predictions = model_response['predictions']
        match_predictions = model_response['match_predictions']
        combo_predictions = model_response['combo_predictions']
        reasoning = model_response['reasoning']

        # Insert into match_predictions table
        supabase.table('match_predictions').insert({
            'fixture_id': fixture_id,
            'half_time_score': predictions['half_time_score']['prediction'],
            'half_time_confidence': predictions['half_time_score']['confidence'],
            'full_time_score': predictions['full_time_score']['prediction'],
            'full_time_confidence': predictions['full_time_score']['confidence'],
            'prediction_1': match_predictions['prediction_1']['prediction'],
            'prediction_1_confidence': match_predictions['prediction_1']['confidence'],
            'prediction_2': match_predictions['prediction_2']['prediction'],
            'prediction_2_confidence': match_predictions['prediction_2']['confidence'],
            'prediction_3': match_predictions['prediction_3']['prediction'],
            'prediction_3_confidence': match_predictions['prediction_3']['confidence'],
            'prediction_4': match_predictions['prediction_4']['prediction'],
            'prediction_4_confidence': match_predictions['prediction_4']['confidence'],
            'prediction_5': match_predictions['prediction_5']['prediction'],
            'prediction_5_confidence': match_predictions['prediction_5']['confidence'],
            'combo_1': combo_predictions['combo_1']['prediction'],
            'combo_1_confidence': combo_predictions['combo_1']['confidence'],
            'combo_2': combo_predictions['combo_2']['prediction'],
            'combo_2_confidence': combo_predictions['combo_2']['confidence'],
            'combo_3': combo_predictions['combo_3']['prediction'],
            'combo_3_confidence': combo_predictions['combo_3']['confidence'],
            'combo_4': combo_predictions['combo_4']['prediction'],
            'combo_4_confidence': combo_predictions['combo_4']['confidence'],
            'combo_5': combo_predictions['combo_5']['prediction'],
            'combo_5_confidence': combo_predictions['combo_5']['confidence'],
            'offensive_analysis': reasoning['offensive_analysis'],
            'defensive_analysis': reasoning['defensive_analysis'],
            'form_analysis': reasoning['form_analysis'],
            'statistical_indicators': reasoning['statistical_indicators'],
            'key_insights': reasoning['key_insights']
        }).execute()

        logging.info(f"Successfully inserted predictions for fixture {fixture_id}")
    except Exception as e:
        logging.error(f"Error inserting match predictions for fixture {fixture_id}: {str(e)}")

async def fetch_prediction_async(session, fixture_id, api_key):
    url = "https://api-football-v1.p.rapidapi.com/v3/predictions"
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
    }
    
    try:
        async with session.get(url, params={"fixture": fixture_id}, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                return fixture_id, data
            return fixture_id, None
    except Exception as e:
        logging.error(f"Error fetching prediction for fixture {fixture_id}: {str(e)}")
        return fixture_id, None

def get_ai_prediction(fixture_id):
    """Check if AI prediction exists for a fixture"""
    try:
        result = supabase.table('match_predictions') \
            .select('id') \
            .eq('fixture_id', fixture_id) \
            .execute()
        return len(result.data) > 0
    except Exception as e:
        logging.error(f"Error checking AI prediction for fixture {fixture_id}: {str(e)}")
        return False

async def fetch_individual_prediction(fixture_id):
    """Fetch and store predictions for a specific fixture."""
    try:
        async with aiohttp.ClientSession() as session:
            api_key = os.getenv('RAPIDAPI_KEY')
            fixture_id_result = await fetch_prediction_async(session, fixture_id, api_key)
            if fixture_id_result[1] and fixture_id_result[1].get('response'):
                success = await store_prediction_async(fixture_id_result[1], fixture_id, supabase)
                if success:
                    logging.info(f"Successfully fetched predictions for fixture {fixture_id}")
                    return True
            logging.warning(f"No predictions found for fixture {fixture_id}")
            return False
    except Exception as e:
        logging.error(f"Error fetching individual prediction for fixture {fixture_id}: {str(e)}")
        return False

def main():
    st.title("⚽ Football Data Manager ⚽")
    
    # Add timezone selector
    selected_timezone = st.selectbox(
        "Select Timezone",
        get_common_timezones(),
        index=1  # Default to Bucharest
    )
    
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
            stored_count = asyncio.run(fetch_and_store_fixtures(selected_date))
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
            
            # Get upcoming fixtures
            current_time = datetime.now(pytz.UTC)
            fixture_ids = []
            
            for fixture in get_major_fixtures_details(selected_date):
                fixture_time = datetime.fromisoformat(fixture['fixture_date'].replace('Z', '+00:00'))
                if fixture_time > current_time:
                    fixture_ids.append(fixture['fixture_id'])
            
            if not fixture_ids:
                st.warning("No upcoming fixtures found for predictions")
                return
            
            # Fetch predictions in batches
            BATCH_SIZE = 5  # Adjust based on API rate limits
            successful_total = 0
            failed_ids = []
            
            total_batches = (len(fixture_ids) + BATCH_SIZE - 1) // BATCH_SIZE
            
            for i in range(0, len(fixture_ids), BATCH_SIZE):
                batch = fixture_ids[i:i + BATCH_SIZE]
                successful, failed = asyncio.run(fetch_predictions_batch(batch))
                successful_total += successful
                failed_ids.extend(failed)
                
                # Update progress (ensure it doesn't exceed 1.0)
                current_batch = (i // BATCH_SIZE) + 1
                progress = min(current_batch / total_batches, 1.0)
                progress_bar.progress(progress)
                
                time.sleep(1)  # Respect API rate limits
            
            if failed_ids:
                st.warning(f"Failed to fetch predictions for {len(failed_ids)} fixtures")
            st.success(f"Successfully stored {successful_total} predictions")
        
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

    # Display major fixtures
    st.subheader("Major Fixtures")
    major_fixtures = get_major_fixtures_details(selected_date, selected_timezone)
    
    if not major_fixtures:
        st.info("No major fixtures found for this date.")
    else:
        for fixture in major_fixtures:
            with st.container():
                col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 2])  # Added col5 for status
                
                # Use the local time from the fixture
                local_time = fixture['local_time'].strftime('%H:%M')
                
                with col1:
                    st.write(f"**{fixture['league_name']}**")
                    st.write(f"🕒 {local_time} ({selected_timezone})")
                
                with col2:
                    st.image(fixture['home_team_logo'], width=30)
                    st.write(fixture['home_team_name'])
                
                with col3:
                    st.image(fixture['away_team_logo'], width=30)
                    st.write(fixture['away_team_name'])
                
                # Check for predictions
                has_prediction = get_fixture_predictions(fixture['fixture_id'])['predictions'] is not None
                has_ai_prediction = get_ai_prediction(fixture['fixture_id'])
                
                with col4:
                    if has_prediction:
                        st.write("🌍✅")
                    else:
                        st.write("🌍❌")
                
                with col5:
                    if has_ai_prediction:
                        st.write("🤖✅")
                    else:
                        st.write("🤖❌")
                with col4:
                    # Button to fetch individual predictions
                    if st.button("Fetch Prediction", key=f"fetch_pred_{fixture['fixture_id']}"):
                        if fetch_individual_prediction(fixture['fixture_id']):
                            st.success(f"Successfully fetched predictions for fixture {fixture['fixture_id']}")
                        else:
                            st.warning(f"No predictions found for fixture {fixture['fixture_id']}")
                
                with col5:
                    if st.button("Ask AI", key=f"ask_ai_{fixture['fixture_id']}"):
                        teams_names = get_teams_names(fixture['fixture_id'])                      
                        data = get_fixture_predictions(fixture['fixture_id'])
                        if data['predictions'] and data['stats']:
                            # Prepare the restructured data as a string
                            restructured_data = f"""
home_team_name: {teams_names['home_team_name']},
away_team_name: {teams_names['away_team_name']},
comp_form_home: {data['predictions']['comp_form_home']},
comp_form_away: {data['predictions']['comp_form_away']},
comp_att_home: {data['predictions']['comp_att_home']},
comp_att_away: {data['predictions']['comp_att_away']},
comp_def_home: {data['predictions']['comp_def_home']},
comp_def_away: {data['predictions']['comp_def_away']},
comp_poisson_home: {data['predictions']['comp_poisson_home']},
comp_poisson_away: {data['predictions']['comp_poisson_away']},
comp_h2h_home: {data['predictions']['comp_h2h_home']},
comp_h2h_away: {data['predictions']['comp_h2h_away']},
comp_goals_home: {data['predictions']['comp_goals_home']},
comp_goals_away: {data['predictions']['comp_goals_away']},
comp_total_home: {data['predictions']['comp_total_home']},
comp_total_away: {data['predictions']['comp_total_away']},
home_team_yellow_cards_first_half_average: {data['stats']['home_team_yellow_cards_first_half_average']},
home_team_yellow_cards_second_half_average: {data['stats']['home_team_yellow_cards_second_half_average']},
home_team_scored_home_first_half_average: {data['stats']['home_team_scored_home_first_half_average']},
home_team_scored_home_second_half_average: {data['stats']['home_team_scored_home_second_half_average']},
home_team_scored_away_first_half_average: {data['stats']['home_team_scored_away_first_half_average']},
home_team_scored_away_second_half_average: {data['stats']['home_team_scored_away_second_half_average']},
home_team_conceded_home_first_half_average: {data['stats']['home_team_conceded_home_first_half_average']},
home_team_conceded_home_second_half_average: {data['stats']['home_team_conceded_home_second_half_average']},
home_team_conceded_away_first_half_average: {data['stats']['home_team_conceded_away_first_half_average']},
home_team_conceded_away_second_half_average: {data['stats']['home_team_conceded_away_second_half_average']},
away_team_yellow_cards_first_half_average: {data['stats']['away_team_yellow_cards_first_half_average']},
away_team_yellow_cards_second_half_average: {data['stats']['away_team_yellow_cards_second_half_average']},
away_team_scored_home_first_half_average: {data['stats']['away_team_scored_home_first_half_average']},
away_team_scored_home_second_half_average: {data['stats']['away_team_scored_home_second_half_average']},
away_team_scored_away_first_half_average: {data['stats']['away_team_scored_away_first_half_average']},
away_team_scored_away_second_half_average: {data['stats']['away_team_scored_away_second_half_average']},
away_team_conceded_home_first_half_average: {data['stats']['away_team_conceded_home_first_half_average']},
away_team_conceded_home_second_half_average: {data['stats']['away_team_conceded_home_second_half_average']},
away_team_conceded_away_first_half_average: {data['stats']['away_team_conceded_away_first_half_average']},
away_team_conceded_away_second_half_average: {data['stats']['away_team_conceded_away_second_half_average']},
                            """

                            # Call the LLM
                            with open("prompt.md", "r") as f:
                                template = f.read()
                            prompt = PromptTemplate.from_template(template)
                            os.environ["ANTHROPIC_API_KEY"] = str(os.getenv("LLM_API_KEY")) 
                            # Initialize LLM
                            llm = ChatAnthropic(
                                model_name="claude-3-5-sonnet-20241022",
                                temperature=0.7,
                                max_tokens=4000,
                                api_key=str(os.getenv("LLM_API_KEY"))
                            )
                            with open("json_example.json", "r") as f:
                                json_example = f.read()
                            complete_prompt = prompt.format(fixture_data=restructured_data, json_example=json_example)
                            print(complete_prompt)
                            model_response = llm.invoke(complete_prompt)
                            model_response_json = json.loads(model_response.content)

                            # Insert the response into the database
                            insert_match_predictions(fixture['fixture_id'], model_response_json)

                            # Display the model response
                            st.success("AI Response generated")
                        else:
                            st.warning("No prediction data available for this fixture")
                
                st.divider()

if __name__ == "__main__":
    main()