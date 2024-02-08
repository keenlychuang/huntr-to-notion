import requests
import pandas as pd
from tqdm import tqdm
import random
import unicodedata
import time
from dotenv import load_dotenv
import os 

# get Notion API key and database ID
load_dotenv() 
notion_api_key = os.getenv('NOTION_SECRET_KEY')
database_id = os.getenv('DATABASE_ID')

# option to add rate limiting 
rate_limiter = .5

# Headers for authentication
headers = {
    "Authorization": f"Bearer {notion_api_key}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"  # Use the latest version available
}

def capitalize_first_letter(string:str) -> str:
    return string[0].upper() + string[1:]

def get_external_icon(): 
    raise NotImplementedError("return a url for an icon")

def get_random_emoji():
    """
    Returns a random emoji from a range of emojis widely supported and accepted by Notion.

    :return: A string containing a random emoji.
    """
    emoji_ranges = [
        (0x1F600, 0x1F64F),  # Emoticons
        (0x1F400, 0x1F4FF),  # Animal & Nature
        (0x1F340, 0x1F37F),  # Additional nature items, like clover, mushroom
    ]

    # Randomly select a range and then a code point within that range
    selected_range = random.choice(emoji_ranges)
    code_point = random.randint(selected_range[0], selected_range[1])

    # Return the character for the selected code point
    # Ensuring the emoji is valid and not part of unassigned code points
    while True:
        try:
            emoji = chr(code_point)
            # Check if the emoji has a valid Unicode name, indicating it's a valid emoji
            unicodedata.name(emoji)
            return emoji
        except ValueError:
            # If current code point is not valid, select a new one within the range
            code_point = random.randint(selected_range[0], selected_range[1])

def huntr_export_to_notion(path:str, emoji:bool = False, external:bool = False, boards = None): 
    """
    Converts a CSV file specified by 'path' into a DataFrame and then into a format suitable for uploading to a Notion database. 
    It then creates pages in the Notion database for each record in the DataFrame.

    :param path: Path to the CSV file to be processed.
    :return: The response from the Notion API after creating the pages.
    """
    frame = huntr_export_to_df(path)
    values = df_to_datavalues(frame, emoji = emoji, external = external, boards = boards)
    response = create_pages(values)
    return response

def huntr_export_to_df(path:str) -> pd.DataFrame:
    """
    Reads a CSV file from the given path and converts it into a pandas DataFrame. 
    Only columns 'title', 'url', 'companyName', and 'listName' are used.

    :param path: Path to the CSV file to be processed.
    :return: A DataFrame containing the processed data.
    """
    useful = {'title', 'url', 'companyName', 'listName', 'boardName'}
    dataframe = pd.read_csv(path, usecols=useful)
    return dataframe

def df_to_datavalues(df: pd.DataFrame, emoji:bool = False, external:bool = False, boards = None): 
    """
    Converts a pandas DataFrame into a list of dictionaries, each representing a page to be created in Notion.
    The DataFrame should contain the columns matching the properties of the Notion database.

    :param df: The DataFrame to be converted.
    :return: A list of dictionaries formatted for the Notion API.
    """
    # convert df to list of dicts
    data = df.to_dict('records')
    # convert list of dicts to list of datavalues
    datavalues = []
    print('creating dataframe')
    for entry in tqdm(data):
        if boards != None and entry['boardName'] not in boards:
            continue 
        properties = {
            "parent": {"database_id": database_id},
            "properties": {
                "Company": {
                    "title": [
                        {
                            "text": {
                                "content": entry['companyName']
                            }
                        }
                    ]
                },
                "Position": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": entry['title']
                            }
                        }
                    ]
                },
                "Status": {
                    "status": {
                        "name": capitalize_first_letter(entry['listName']) 
                    }
                }
            }
        }
        if emoji: 
            properties['icon'] = {
                "type" : "emoji",
                "emoji": get_random_emoji() 
            }
        # if external: 
        #     properties['icon'] = {
        #         "type" : "external",
        #         "external": { "url" : get_external_icon()}
        #     }
        datavalues.append(properties) 
    return datavalues

def count_database_entries(): 
    headers = {
        "Authorization": f"Bearer {notion_api_key}",
        "Notion-Version": "2022-06-28"
    }
    url = f"https://api.notion.com/v1/databases/{database_id}/query"

    count = 0
    has_more = True
    next_cursor = None

    while has_more:
        if next_cursor:
            response = requests.post(url, headers=headers, json={"start_cursor": next_cursor})
        else:
            response = requests.post(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            count += len(data["results"])
            has_more = data["has_more"]
            next_cursor = data.get("next_cursor")
        else:
            print("Failed to fetch database entries")
            break

    return count

def create_pages(datavalues):
    """
    Creates pages in the Notion database for each entry in 'datavalues'.
    Each entry in 'datavalues' should be a dictionary formatted for the Notion API.

    :param datavalues: A list of dictionaries, each representing a page to be created.
    :return: The response from the Notion API after creating the last page.
    """
    # create pages
    print('creating entries')
    for entry in tqdm(datavalues):
        count = 0 
        max_retries = 3
        success = False 
        while count < max_retries and not success: 
            try: 
                time.sleep(rate_limiter*count)
                response = requests.post(
                        "https://api.notion.com/v1/pages", 
                        json=entry, 
                        headers=headers
                )
                response.raise_for_status() 
                success = True 
            except: 
                # error, try again 
                count+=1 
        if not success: 
            print(entry)
            raise Exception(f"failed after {max_retries} attempts")
    return 

def add_test_value(): # Function to add a test value to the database
    # Data for the new entry
    data = {
        "parent": {"database_id": database_id},
        "properties": {
            "Company": {
                "title": [
                    {
                        "text": {
                            "content": "Zed"
                        }
                    }
                ]
            },
            "Position": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "Master of Shadows"
                        }
                    }
                ]
            },
            "Status": {
                "status": {
                    "name": "Wishlist"
                }
            }
        }
    }

    # API endpoint to create a page (add an entry)
    response = requests.post('https://api.notion.com/v1/pages', headers=headers, json=data)

    # Check response
    if response.status_code == 200:
        print("Entry added successfully.")
    else:
        print("Failed to add entry:", response.json()) 

def query_values(): 
    # API endpoint to retrieve a database
    url = f'https://api.notion.com/v1/databases/{database_id}'

    # Make the requestß
    response = requests.get(url, headers=headers)

    # Check response
    if response.status_code == 200:
        database_info = response.json()
        print("Database Properties:")
        for property_name, property_info in database_info['properties'].items():
            print(f"- {property_name}: {property_info['type']}")
    else:
        print("Failed to retrieve database:", response.json())