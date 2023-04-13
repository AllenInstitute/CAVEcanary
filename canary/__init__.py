import asyncio
import configparser
import os
import random

import numpy as np
import pandas as pd
from caveclient import CAVEclient
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

__version__ = "0.3.0"

class Canary:
    def __init__(self, client=None, config=None, slack_client=None):
        """
        Initializes the `Canary` instance.

        Parameters:
            client (CAVEclient, optional): The CAVEclient instance to use. Defaults to `None`.
            config (configparser.ConfigParser, optional): The configuration to use. Defaults to `None`.
            slack_client (slack_sdk.WebClient, optional): The Slack client to use. Defaults to `None`.
        """
        if not config:
            self.config = configparser.ConfigParser()
            config_file = os.environ.get("CAVECANARY_CONFIG_FILE", "config.cfg")
            self.config.read(config_file)
        else:
            self.config = config
        self.database_uri = self.config.get("SETTINGS", "DATABASE_URI")
        self.datastack_name = self.config.get("SETTINGS", "DATASTACK_NAME")
        self.server_address = self.config.get(
            "SETTINGS", "SERVER_ADDRESS", fallback=None
        )
        self.slack_api_token = self.config.get("SETTINGS", "SLACK_API_TOKEN")
        self.slack_channel = self.config.get("SETTINGS", "SLACK_CHANNEL")
        self.check_interval = self.config.getint("SETTINGS", "CHECK_INTERVAL")
        self.num_test_annotations = self.config.getint(
            "SETTINGS", "NUM_TEST_ANNOTATIONS"
        )
        if client is None:
            self.client = CAVEclient(
                self.datastack_name,
                write_server_cache=False,
            )
        else:
            self.client = client
        self.datastack_info = self.client.info.get_datastack_info(self.datastack_name)
        if slack_client is None:
            self.slack_client = WebClient(token=self.slack_api_token)
        else:
            self.slack_client = slack_client
        self.segmentation_source = self.datastack_info["segmentation_source"].split(
            "/"
        )[-1]

    async def run(self, iterations=None):
        """
        Runs the Canary.

        Parameters:
            iterations (int, optional): The number of iterations to run. Defaults to `None`.
        """
        iteration_count = 0
        background_tasks = set()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        while True:
            if iterations is not None and iteration_count >= iterations:
                break
            # check if event loop is running and add as task else run with eventloop
            if loop and loop.is_running():
                task = loop.create_task(self.check_random_annotations())
                background_tasks.add(task)
                await task
                task.add_done_callback(background_tasks.discard)
            else:
                asyncio.run(self.check_random_annotations())
            await asyncio.sleep(self.check_interval)

            iteration_count += 1

    async def check_random_annotations(self):
        """
        Checks the specified random annotations.
        """
        annotation_tables = self.client.materialize.get_tables()
        version_info = self.client.materialize.get_version_metadata()

        async_engine = create_async_engine(self.database_uri)
        for table_name in annotation_tables:
            table_info = self.client.materialize.get_table_metadata(table_name)
            if not version_info["is_merged"] and table_info.get("annotation_table"):
                table_name = f"{table_name}__{self.segmentation_source}"
            elif not version_info["is_merged"]:
                continue

            has_error = await self.query_data_and_check_roots(async_engine, table_name)
        return has_error

    async def query_data_and_check_roots(
        self, async_engine, table_name, sample_percent=10
    ):
        """
        Queries the specified data and checks the roots.

        Parameters:
            async_engine (sqlalchemy.ext.asyncio.engine.AsyncEngine): The async engine to use.
            table_name (str): The name of the table to use.
            sample_percent (int, optional): The percentage to sample. Defaults to `10`.

        Returns:
            bool: `True` if an error was found; `False` otherwise.
        """
        async with async_engine.begin() as conn:
            sample_query = f"SELECT * FROM {table_name} TABLESAMPLE BERNOULLI({sample_percent}) LIMIT {self.num_test_annotations}"

            try:
                result = await conn.execute(text(sample_query))
            except Exception as e:
                raise (e)

            df = pd.DataFrame(result)

            has_error = self.check_root_ids(df)

            # close database connection
        await async_engine.dispose()
        return has_error

    def check_root_ids(self, df):
        """
        Checks the root IDs in the specified DataFrame.

        Parameters:
            df (pandas.DataFrame): The DataFrame to use.

        Returns:
            bool: `True` if an error was found; `False` otherwise.
        """
        supervoxel_columns = [
            col for col in df.columns if col.endswith("_supervoxel_id")
        ]
        rootid_columns = [col for col in df.columns if col.endswith("_rootid")]
        errors_found = False
        for supervoxel_col in supervoxel_columns:
            prefix = supervoxel_col[: -len("_supervoxel_id")]
            rootid_col = f"{prefix}_rootid"

            if rootid_col not in rootid_columns:
                continue

            try:
                root_ids = self.client.chunkedgraph.get_roots(
                    df[supervoxel_col].values,
                    timestamp=self.client.materialize.get_version_metadata()[
                        "time_stamp"
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
                errors_found = True
        return errors_found

    def send_slack_notification(self, message):
        """
        Sends a Slack notification with the specified message.

        Parameters:
            message (str): The message to send.
        """
        try:
            self.slack_client.chat_postMessage(channel=self.slack_channel, text=message)
        except SlackApiError as e:
            print(f"Error sending Slack message: {e}")


if __name__ == "__main__":
    canary = Canary()
    loop = asyncio.new_event_loop()
    asyncio.get_event_loop().run_until_complete(canary.run())
    loop.close()
