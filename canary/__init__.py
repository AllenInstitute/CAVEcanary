__version__ = "0.1.0"
import time
import random
from caveclient import CAVEclient
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import configparser
import os


class Canary:
    def __init__(self, client=None):

        self.config = configparser.ConfigParser()
        config_file = os.environ.get("CAVECANARY_CONFIG_FILE", "config.cfg")
        self.config.read(config_file)

        self.datastack_name = self.config.get("Settings", "DATASTACK_NAME")
        self.server_address = self.config.get(
            "Settings", "SERVER_ADDRESS", fallback=None
        )
        self.slack_api_token = self.config.get("Settings", "SLACK_API_TOKEN")
        self.slack_channel = self.config.get("Settings", "SLACK_CHANNEL")
        self.check_interval = self.config.getint("Settings", "CHECK_INTERVAL")
        self.num_test_annotations = self.config.getint(
            "Settings", "NUM_TEST_ANNOTATIONS"
        )
        if client is None:
            self.client = CAVEclient(
                self.datastack_name, server_address=self.server_address
            )
        else:
            self.client = client

        self.slack_client = WebClient(token=self.slack_api_token)

    def run(self):
        self.check_random_annotations()
        time.sleep(self.check_interval)

    def check_random_annotations(self):
        print(self.client)
        tables = self.client.materialize.get_tables()
        for table in tables:
            num_rows = self.client.materialize.get_annotation_count(table)
            # make the offset something that is not too close to the end
            max_offset = max(num_rows - self.num_test_annotations, 0)
            offset = random.randint(0, max_offset)

            try:
                df = self.client.materialize.query_table(
                    table, offset=offset, limit=self.num_test_annotations
                )
            except Exception as e:
                self.send_slack_notification(f"Error in query_table: {e}")
                return

            self.check_root_ids(df)

    def check_root_ids(self, df):
        supervoxel_columns = [
            col for col in df.columns if col.endswith("_supervoxel_id")
        ]
        rootid_columns = [col for col in df.columns if col.endswith("_rootid")]

        for supervoxel_col in supervoxel_columns:
            prefix = supervoxel_col[: -len("_supervoxel_id")]
            rootid_col = f"{prefix}_rootid"

            if rootid_col not in rootid_columns:
                continue

            try:
                root_ids = self.client.chunkedgraph.get_roots(
                    df[supervoxel_col].values,
                    timestamp=self.client.materialize.get_version_metadata()[
                        "timestamp"
                    ],
                )
            except Exception as e:
                self.send_slack_notification(f"Error in get_roots: {e}")
                continue

            mismatch = df[rootid_col].values != root_ids
            if mismatch.any():
                self.send_slack_notification(
                    f"Mismatch found in {rootid_col}: {mismatch}"
                )

    def send_slack_notification(self, message):
        try:
            self.slack_client.chat_postMessage(
                channel=config.SLACK_CHANNEL, text=message
            )
        except SlackApiError as e:
            print(f"Error sending Slack message: {e}")
