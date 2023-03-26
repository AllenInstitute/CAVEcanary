
# CAVECanary Service

CAVECanary is a Python-based service that monitors a Materialization service and checks if the data managed by the service is consistent. If Canary discovers that Materialization is down or if a query returns inconsistent data, it will send a notification via Slack.

## Requirements

- Python 3.7 or later
- A Slack API token with the `chat:write` scope

## Installation

1. Clone this repository:

git clone https://github.com/AllenInstitute/CAVEcanery.git
cd CAVEcanery

2. Install the required Python packages:
pip install -r requirements.txt


3. Set up the configuration by creating a `config.py` file with the following content:

```python

DATASTACK_NAME = "your_datastack_name"
SLACK_API_TOKEN = "your_slack_api_token"
SLACK_CHANNEL = "your_slack_channel"
NUM_SYNAPSES = 100  # You can change this value to the desired number of synapses to query
```


## Running the Canary Service
To run the Canary service locally:

```
python main.py
```
