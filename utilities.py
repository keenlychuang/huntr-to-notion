import requests
import pandas as pd
from tqdm import tqdm
import random
import unicodedata
import time

# get Notion API key and database ID
#TODO 

# Notion seems to complain when I send too many requests, so let's do some rate limiting. 
rate_limiter = 1

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
    Returns a random emoji from a broad range of Unicode emojis.

    :return: A string containing a random emoji.
    """
    emoji_ranges = [
        (0x1F601, 0x1F64F),  # Emoticons
        (0x1F300, 0x1F5FF),  # Misc Symbols and Pictographs
        (0x1F680, 0x1F6FF),  # Transport and Map
        (0x2600, 0x26FF),    # Misc Symbols
        (0x2700, 0x27BF),    # Dingbats
        (0x1F900, 0x1F9FF)   # Supplemental Symbols and Pictographs
    ]

    # Randomly select a range and then a code point within that range
    range = random.choice(emoji_ranges)
    code_point = random.randint(range[0], range[1])

    # Return the character for the selected code point
    # We use a while loop to skip over unassigned code points
    while True:
        try:
            emoji = chr(code_point)
            if unicodedata.name(emoji).startswith("CANCELED"):
                code_point = random.randint(range[0], range[1])
                continue
            return emoji
        except ValueError:
            # Move to next code point if current is not valid
            code_point = random.randint(range[0], range[1])

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
        if external: 
            properties['icon'] = {
                "type" : "external",
                "external": { "url" : get_external_icon()}
            }
        datavalues.append(properties) 
    return datavalues

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
        time.sleep(rate_limiter)
        response = requests.post(
            "https://api.notion.com/v1/pages", 
            json=entry, 
            headers=headers
        )
        response.raise_for_status()
    return response

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