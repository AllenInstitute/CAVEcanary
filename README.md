
# CAVECanary Service

CAVECanary is a Python-based service that monitors a Materialization service and checks if the data managed by the service is consistent. If Canary discovers that Materialization is down or if a query returns inconsistent data, it will send a notification via Slack.

## Requirements

- Python 3.7 or later
- A Slack API token with the `chat:write` scope

## Installation

1. Clone this repository:

git clone https://github.com/AllenInstitute/CAVEcanary.git
cd CAVEcanary

2. Install the required Python packages:
pip install -r requirements.txt


3. Set up the configuration by creating a `config.cfg` file with the following content:

```python

[SETTINGS]
DATASTACK_NAME = "your_datastack_name"
SERVER_ADDRESS = "your_global_cave_server_address"
DATABASE_URI = "postgresql+asyncpg://your_database_user_and_password/database_name" # Note: postgresql+asyncpg driver required
SLACK_API_TOKEN = "your_slack_api_token"
SLACK_CHANNEL = "your_slack_channel"
NUM_TEST_ANNOTATIONS = 100  # You can change this value to the desired number of synapses to query"
CHECK_INTERVAL = 60 # Number of seconds to run check
USE_TSM_SYSTEM_ROWS = True # Use postgres 'TSM_SYSTEM_ROWS' extension to randomly sample rows
```


## Running the Canary Service
To run the Canary service locally:

```
python main.py
```
