__version__ = "0.3.0"
import time
import random
import os
import asyncio
import configparser
from urllib.parse import urlparse

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
        self.database_uri = self.config.get("Settings", "DATABASE_URI")
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
        self.test_mode = self.config.getboolean("Settings", "TEST_MODE")
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
        tables = self.client.materialize.get_tables()

        async_engine = create_async_engine(self.database_uri)
        for table in tables:
            async with async_engine.begin() as conn:
                # specify table name
                try:
                    if self.test_mode or not self.is_postgres_url(self.database_uri):
                        df = self.query_mat_table(table)
                    else:
                        # get a random sample of 1000 rows from the table
                        sample_query = f"SELECT * FROM {table} TABLESAMPLE BERNOULLI(10) LIMIT {self.num_test_annotations}"
                        result = await conn.execute(text(sample_query))

                        # fetch the random sample of 1000 rows as a list of dictionaries
                        data = [dict(row) async for row in result]
                        df = pd.DataFrame(data)
                except Exception as query_error:
                    self.send_slack_notification(f"Error in query_table: {query_error}")
                    return

                self.check_root_ids(df)

            # close database connection
            await async_engine.dispose()

    def query_mat_table(self, table):
        num_rows = self.client.materialize.get_annotation_count(table)
        # make the offset something that is not too close to the end
        max_offset = max(num_rows - self.num_test_annotations, 0)
        offset = random.randint(0, max_offset)

        if num_rows < 100000:
            df = self.client.materialize.query_table(
                table, offset=offset, limit=self.num_test_annotations
            )
        else:
            df = self.client.materialize.query_table(
                table,
                filter_in_dict={
                    "id": np.arange(offset, offset + self.num_test_annotations)
                },
            )
        return df

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
            self.slack_client.chat_postMessage(channel=self.slack_channel, text=message)
        except SlackApiError as e:
            print(f"Error sending Slack message: {e}")

    def is_postgres_url(self, url):
        parsed_url = urlparse(url)
        return parsed_url.scheme.startswith("postgres")
