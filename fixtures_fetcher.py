import os
import requests
import time
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv
from functools import wraps

load_dotenv()
# Supabase credentials
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# API credentials
API_KEY = os.getenv("FIXTURES_API_KEY")
API_HOST = "api-football-v1.p.rapidapi.com"

def retry_on_failure(max_attempts=3, delay_seconds=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        print(f"Failed after {max_attempts} attempts: {e}")
                        return False
                    print(f"Attempt {attempts} failed: {e}. Retrying in {delay_seconds} seconds...")
                    time.sleep(delay_seconds)
            return False
        return wrapper
    return decorator

@retry_on_failure(max_attempts=3, delay_seconds=1)
def upsert_fixture(fixture):
    """
    Upserts a fixture into the Supabase database with retry mechanism.
    """
    try:
        fixture_data = {
            "fixture_id": fixture["fixture"]["id"],
            "referee": fixture["fixture"]["referee"],
            "timezone": fixture["fixture"]["timezone"],
            "date": fixture["fixture"]["date"],
            "timestamp": fixture["fixture"]["timestamp"],
            "status_long": fixture["fixture"]["status"]["long"],
            "status_short": fixture["fixture"]["status"]["short"],
            "goals_home": fixture["goals"]["home"],
            "goals_away": fixture["goals"]["away"],
            "halftime_home": fixture["score"]["halftime"]["home"],
            "halftime_away": fixture["score"]["halftime"]["away"],
            "fulltime_home": fixture["score"]["fulltime"]["home"],
            "fulltime_away": fixture["score"]["fulltime"]["away"],
            "extratime_home": fixture["score"]["extratime"]["home"],
            "extratime_away": fixture["score"]["extratime"]["away"],
            "penalty_home": fixture["score"]["penalty"]["home"],
            "penalty_away": fixture["score"]["penalty"]["away"],

            # Venue information
            "venue_id": fixture["fixture"]["venue"]["id"],
            "venue_name": fixture["fixture"]["venue"]["name"],
            "venue_city": fixture["fixture"]["venue"]["city"],

            # League information
            "league_id": fixture["league"]["id"],
            "league_name": fixture["league"]["name"],
            "league_country": fixture["league"]["country"],
            "league_logo": fixture["league"]["logo"],
            "league_flag": fixture["league"]["flag"],
            "league_season": fixture["league"]["season"],
            "league_round": fixture["league"]["round"],

            # Team information
            "home_team_id": fixture["teams"]["home"]["id"],
            "home_team_name": fixture["teams"]["home"]["name"],
            "home_team_logo": fixture["teams"]["home"]["logo"],
            "away_team_id": fixture["teams"]["away"]["id"],
            "away_team_name": fixture["teams"]["away"]["name"],
            "away_team_logo": fixture["teams"]["away"]["logo"],
        }

        # Upsert data into the fixtures table
        supabase.table("fixtures").upsert(fixture_data).execute()
        print(f"Successfully upserted fixture {fixture['fixture']['id']}")
        return True

    except KeyError as e:
        print(f"Error accessing fixture data: {e}")
        print(f"Problematic fixture: {fixture['fixture']['id'] if 'fixture' in fixture and 'id' in fixture['fixture'] else 'Unknown ID'}")
        return False
    except Exception as e:
        print(f"Error upserting fixture: {e}")
        return False

def fetch_fixtures():
    """
    Fetches fixtures from the API and upserts them into the database.
    """
    # Get today's date in YYYY-MM-DD format
    today = datetime.utcnow().strftime("%Y-%m-%d")

    try:
        # Fetch fixtures from the API
        url = f"https://{API_HOST}/v3/fixtures?date={today}"
        headers = {
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": API_HOST,
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        
        success_count = 0
        total_fixtures = len(data["response"])
        
        print(f"Found {total_fixtures} fixtures for {today}")
        
        # Upsert each fixture into the database
        for fixture in data["response"]:
            if upsert_fixture(fixture):
                success_count += 1
        
        print(f"Successfully processed {success_count}/{total_fixtures} fixtures for {today}")

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch fixtures: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    fetch_fixtures()