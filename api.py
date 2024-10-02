import requests

# Constants for Group-IB API
GROUP_IB_API_URL = "https://api.group-ib.com/endpoint"  # Replace with actual Group-IB API endpoint
API_KEY = "your_group_ib_api_key"  # Replace with your API key

def fetch_data_from_group_ib():
    """Fetch data from Group-IB API using REST."""
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        # Make the request to the Group-IB API
        response = requests.get(GROUP_IB_API_URL, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse and return the JSON data
            data = response.json()
            return data
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data from Group-IB API: {e}")
        return None

if __name__ == '__main__':
    # Fetch and print the data from the Group-IB API
    data = fetch_data_from_group_ib()
    if data:
        print("Data fetched successfully:")
        print(data)
