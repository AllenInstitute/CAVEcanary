import time
import random
import pandas as pd
from caveclient import CAVEclient
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import config


class Canary:
    def __init__(self):
        self.client = CAVEclient(config.DATASTACK_NAME)
        self.slack_client = WebClient(token=config.SLACK_API_TOKEN)

    def check_random_annotations(self):
        tables = self.client.materialize.get_tables()
        for table in tables:
            num_rows = self.client.materialize.get_annotation_count(table)
            offset = random.randint(0, num_rows - config.NUM_SYNAPSES)
            try:
                df = self.client.materialize.query_table(
                    table, offset=offset, limit=config.NUM_SYNAPSES
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