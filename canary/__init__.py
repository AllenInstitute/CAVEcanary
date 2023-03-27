__version__ = "0.3.0"
import time
import random
import os
import asyncio
import configparser

import numpy as np
import pandas as pd
from sqlalchemy import Table, text, MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from caveclient import CAVEclient
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class Canary:
    def __init__(self, client=None):

        self.config = configparser.ConfigParser()
        config_file = os.environ.get("CAVECANARY_CONFIG_FILE", "config.cfg")
        self.config.read(config_file)
        self.database_uri = self.config.get("DATABASE_URI")
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
                self.datastack_name,
                server_address=self.server_address,
                write_server_cache=False,
            )
        else:
            self.client = client

        self.slack_client = WebClient(token=self.slack_api_token)

    def run(self):
        while True:
            asyncio.run(self.check_random_annotations())
            time.sleep(self.check_interval)

    async def check_random_annotations(self):
        # create engine and connect to database
        
        tables = self.client.materialize.get_tables()
        
        async_engine = create_async_engine(self.database_uri)
        for table in tables:
            async with async_engine.begin() as conn:
                # specify table name
                table_name = 'my_table'

                # get a random sample of 1000 rows from the table
                sample_query = f"SELECT * FROM {table} TABLESAMPLE BERNOULLI(10) LIMIT {self.num_test_annotations}"
                try:
                    result = await conn.execute(text(sample_query))
                except Exception as e:
                    self.send_slack_notification(f"Error in query_table: {e}")
                    return

                    # fetch the random sample of 1000 rows as a list of dictionaries
                rows = [dict(row) async for row in result]
                self.check_root_ids(pd.DataFrame(rows))

            # close database connection
            await async_engine.dispose()

        
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
                channel=self.slack_channel, text=message)
        except SlackApiError as e:
            print(f"Error sending Slack message: {e}")
