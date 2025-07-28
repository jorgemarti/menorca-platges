#!/usr/bin/env python3
"""
Beach Parking Status Scraper with Supabase Integration
Enhanced version for GitHub Actions with better logging and error handling.
"""

import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import sys
import os
import logging
from supabase import create_client, Client

# Configure logging for GitHub Actions
def setup_logging():
    """Set up logging with appropriate format for GitHub Actions."""
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('parking_monitor.log')
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def setup_supabase():
    """
    Initialize Supabase client using environment variables.
    """
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    
    if not url or not key:
        logger.error("SUPABASE_URL and SUPABASE_KEY environment variables must be set")
        return None
    
    try:
        client = create_client(url, key)
        logger.info("Successfully initialized Supabase client")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")
        return None

def save_to_supabase(supabase: Client, parking_data):
    """
    Save parking data to Supabase database.
    """
    try:
        # Prepare data for database insertion
        db_records = []
        for item in parking_data:
            db_record = {
                'recorded_at': item['Date'],
                'beach_name': item['Beach'],
                'status': item['Status']
            }
            db_records.append(db_record)
        
        logger.info(f"Attempting to save {len(db_records)} records to database")
        
        # Insert all records at once
        result = supabase.table('parking_status').insert(db_records).execute()
        
        if result.data:
            logger.info(f"Successfully saved {len(result.data)} records to database")
            
            # Log the data for debugging
            for record in result.data:
                logger.info(f"Saved: {record['beach_name']} - {record['status']} at {record['recorded_at']}")
            
            return True
        else:
            logger.warning("No data was inserted to database")
            return False
            
    except Exception as e:
        logger.error(f"Error saving to Supabase: {e}")
        return False

def scrape_parking_status():
    """
    Scrapes the parking status from the website and returns JSON data.
    """
    url = "http://mout.cime.es/ParkingsPlatges.aspx"
    
    try:
        logger.info(f"Fetching data from {url}")
        
        # Send GET request to the website
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        logger.info(f"Successfully fetched page (status: {response.status_code})")
        
        # Parse HTML content
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all PLA_linia div classes
        pla_linia_divs = soup.find_all('div', class_='PLA_linia')
        
        if not pla_linia_divs:
            logger.warning("No 'PLA_linia' divs found on the page")
            return []
        
        logger.info(f"Found {len(pla_linia_divs)} PLA_linia divs")
        
        # Get current date and time
        current_datetime = datetime.now().isoformat()
        
        parking_data = []
        
        for i, div in enumerate(pla_linia_divs):
            # Find the beach label (ct100_Content1_Label pattern)
            beach_label = div.find('span', id=lambda x: x and 'Content1_Label' in x)
            
            # Find the status label (ct100_Content pattern)
            status_label = div.find('span', id=lambda x: x and 'Content1_lb' in x and 'Label' not in x)
            
            if beach_label and status_label:
                beach_name = beach_label.get_text(strip=True)
                status = status_label.get_text(strip=True)
                
                parking_info = {
                    "Date": current_datetime,
                    "Beach": beach_name,
                    "Status": status
                }
                
                parking_data.append(parking_info)
                logger.info(f"Parsed beach {i+1}: {beach_name} - {status}")
            else:
                logger.warning(f"Could not find beach or status label in div {i+1}")
        
        logger.info(f"Successfully parsed {len(parking_data)} parking records")
        return parking_data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching the website: {e}")
        return []
    except Exception as e:
        logger.error(f"Error processing the data: {e}")
        return []

def main():
    """
    Main function to run the scraper and save to database.
    """
    logger.info("Starting beach parking status monitor")
    
    # Check command line arguments
    save_to_db = '--no-db' not in sys.argv
    output_json = '--json' in sys.argv
    
    # GitHub Actions runs in UTC, log the current time
    logger.info(f"Current UTC time: {datetime.utcnow()}")
    
    parking_data = scrape_parking_status()
    
    if not parking_data:
        logger.error("No parking data found or error occurred")
        sys.exit(1)
    
    # Save to Supabase if requested
    if save_to_db:
        supabase = setup_supabase()
        if supabase:
            success = save_to_supabase(supabase, parking_data)
            if not success:
                logger.error("Failed to save data to database")
                sys.exit(1)
        else:
            logger.error("Skipping database save due to configuration error")
            sys.exit(1)
    
    # Output JSON if requested or if not saving to database
    if output_json or not save_to_db:
        json_output = json.dumps(parking_data, indent=2, ensure_ascii=False)
        print(json_output)
        logger.info("JSON output generated")
    
    logger.info(f"Successfully processed {len(parking_data)} parking records")
    
    # GitHub Actions workflow summary
    if os.getenv('GITHUB_ACTIONS'):
        summary = f"""
## Beach Parking Monitor Results

- **Execution Time**: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
- **Records Processed**: {len(parking_data)}
- **Beaches Monitored**: {', '.join([item['Beach'] for item in parking_data])}

### Current Status:
"""
        for item in parking_data:
            summary += f"- **{item['Beach']}**: {item['Status']}\n"
        
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as f:
            f.write(summary)

if __name__ == "__main__":
    main()
