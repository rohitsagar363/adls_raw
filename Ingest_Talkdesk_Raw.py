# ==============================================================================
# DATABRICKS NOTEBOOK: Talkdesk Raw Ingestion (Async Driver Mode)
# DESCRIPTION: Ingests Interaction data to ADLS Gen2 without Spark Job overhead.
# ==============================================================================

import asyncio
import aiohttp
import json
from datetime import datetime, timedelta
from tenacity import retry, wait_exponential, stop_after_attempt

# ------------------------------------------------------------------------------
# 1. ENVIRONMENT SETUP & JVM HACKS
# ------------------------------------------------------------------------------
# We access the underlying Hadoop FileSystem directly via the Spark Driver's JVM.
# This allows us to write to ABFSS using Cluster Auth, but without starting Spark Tasks.
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
conf = sc._jsc.hadoopConfiguration()

# ------------------------------------------------------------------------------
# 2. CONFIGURATION & WIDGETS
# ------------------------------------------------------------------------------
dbutils.widgets.text("start_date", "2023-06-01", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2023-12-01", "End Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "backfill"], "Execution Mode")
dbutils.widgets.text("storage_account", "YOUR_STORAGE_ACCOUNT_NAME", "Storage Account Name")
dbutils.widgets.text("container", "raw", "Container Name")

# GET SECRETS (Ensure you created these in Databricks Secrets)
try:
    # Change 'talkdesk_scope' and 'api_token' to match your secret scope
    TALKDESK_TOKEN = dbutils.secrets.get(scope="talkdesk_scope", key="api_token")
except:
    TALKDESK_TOKEN = "REPLACE_WITH_TOKEN_IF_TESTING_MANUALLY"

# CONSTANTS
BASE_URL = "https://api.talkdeskapp.com/interactions"
MAX_CONCURRENCY = 5  # Safe limit to avoid 429s

# PATH CONSTRUCTION
storage_acct = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
ROOT_PATH = f"abfss://{container}@{storage_acct}.dfs.core.windows.net/talkdesk/interactions"

# ------------------------------------------------------------------------------
# 3. HELPER FUNCTIONS
# ------------------------------------------------------------------------------

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
async def fetch_page(session, url, params=None):
    """Fetches a single page from Talkdesk API with Rate Limit handling."""
    headers = {
        "Authorization": f"Bearer {TALKDESK_TOKEN}",
        "Content-Type": "application/json"
    }
    async with session.get(url, headers=headers, params=params) as response:
        if response.status == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"⚠️ Rate limit hit. Sleeping {retry_after}s...")
            await asyncio.sleep(retry_after)
            raise Exception("Rate limit hit")
        response.raise_for_status()
        return await response.json()

def write_to_adls_native(content_str, file_path_str):
    """
    Writes string content directly to ADLS using Hadoop FS (Driver-side).
    """
    try:
        fs = FileSystem.get(URI(file_path_str), conf)
        out_stream = fs.create(Path(file_path_str), True) # True = Overwrite
        out_stream.write(content_str.encode('utf-8'))
        out_stream.close()
    except Exception as e:
        print(f"❌ FS Write Error: {e}")
        raise e

async def process_hourly_window(semaphore, start_date, end_date):
    """
    1. Fetches ALL pages for a specific hour.
    2. Aggregates them in memory.
    3. Writes ONE file to ADLS.
    """
    async with semaphore: 
        async with aiohttp.ClientSession() as session:
            current_url = BASE_URL
            params = {"page": 1, "per_page": 50, "start_date": start_date, "end_date": end_date}
            all_records = []
            
            while current_url:
                try:
                    data = await fetch_page(session, current_url, params)
                    
                    # EXTRACT DATA
                    items = data.get('_embedded', {}).get('interactions', [])
                    if items:
                        all_records.extend(items)
                    
                    # PAGINATION
                    if data.get('_links', {}).get('next'):
                        current_url = data['_links']['next']['href']
                        params = None 
                    else:
                        current_url = None 
                        
                except Exception as e:
                    print(f"❌ Failed chunk {start_date}: {str(e)}")
                    break
            
            # WRITE TO ADLS
            if all_records:
                dt_obj = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
                
                # Folder: YYYY/MM/DD
                # File: HHMM_interactions.json
                file_path = (f"{ROOT_PATH}/{dt_obj.year}/{dt_obj.month:02d}/"
                             f"{dt_obj.day:02d}/{dt_obj.strftime('%H%M')}_interactions.json")
                
                # Offload blocking IO to a thread
                await asyncio.to_thread(write_to_adls_native, json.dumps(all_records), file_path)
                print(f"✅ Saved {len(all_records)} recs to {dt_obj.strftime('%Y-%m-%d %H:%M')}")
            else:
                print(f"∅ No data for {start_date}")

# ------------------------------------------------------------------------------
# 4. MAIN EXECUTION LOOP
# ------------------------------------------------------------------------------

async def main():
    mode = dbutils.widgets.get("mode")
    tasks = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    if mode == "incremental":
        # Calculate PREVIOUS hour (e.g. at 10:00, run for 09:00-10:00)
        now = datetime.utcnow()
        end_dt = now.replace(minute=0, second=0, microsecond=0)
        start_dt = end_dt - timedelta(hours=1)
        
        print(f"🕒 Incremental Run: {start_dt} -> {end_dt}")
        tasks.append(process_hourly_window(
            semaphore, 
            start_dt.strftime("%Y-%m-%dT%H:%M:%S"), 
            end_dt.strftime("%Y-%m-%dT%H:%M:%S")
        ))

    elif mode == "backfill":
        s_str = dbutils.widgets.get("start_date")
        e_str = dbutils.widgets.get("end_date")
        s_date = datetime.strptime(s_str, "%Y-%m-%d")
        e_date = datetime.strptime(e_str, "%Y-%m-%d")
        
        print(f"📚 Backfill Run: {s_date} -> {e_date}")
        
        curr = s_date
        while curr < e_date:
            nxt = curr + timedelta(hours=1)
            # Create a task for every hour
            tasks.append(process_hourly_window(
                semaphore, 
                curr.strftime("%Y-%m-%dT%H:%M:%S"), 
                nxt.strftime("%Y-%m-%dT%H:%M:%S")
            ))
            curr = nxt

    # Execute all tasks
    await asyncio.gather(*tasks)
    print("🏁 Ingestion Complete.")

if __name__ == "__main__":
    await main()
