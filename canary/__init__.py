import asyncio
import configparser
import os
import logging

import numpy as np
import pandas as pd
from caveclient import CAVEclient
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.engine.url import make_url

__version__ = "0.4.4"


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
        self.default_uri = self.config.get("SETTINGS", "DATABASE_URI")
        self.datastack_name = self.config.get("SETTINGS", "DATASTACK_NAME")
        self.server_address = self.config.get(
            "SETTINGS", "SERVER_ADDRESS", fallback=None
        )
        self.slack_api_token = self.config.get("SETTINGS", "SLACK_API_TOKEN")
        self.slack_channel = self.config.get("SETTINGS", "SLACK_CHANNEL")
        self.use_tsm_system_rows_extension = self.config.get(
            "SETTINGS", "USE_TSM_SYSTEM_ROWS", fallback=False
        )

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

        self.database_uri = self._create_latest_version_db_uri()

        self.datastack_info = self.client.info.get_datastack_info(self.datastack_name)
        if slack_client is None:
            self.slack_client = WebClient(token=self.slack_api_token)
        else:
            self.slack_client = slack_client
        self.segmentation_source = self.datastack_info["segmentation_source"].split(
            "/"
        )[-1]

    async def check_if_extension_is_installed(
        self, extension_name: str = "tsm_system_rows"
    ):
        # check if postgresql extension is installed on database
        async_engine = create_async_engine(self.database_uri)
        async with async_engine.begin() as conn:
            get_extension_query = (
                f"SELECT * FROM pg_extension WHERE extname = '{extension_name}';"
            )
            result = await conn.execute(text(get_extension_query))
            extension_results = result.fetchone()
            logging.debug(extension_results)
            if not extension_results:
                try:
                    result = await conn.execute(
                        text(f"CREATE EXTENSION {extension_name};")
                    )
                    logging.debug(result.fetchone())
                    logging.debug(f"Created extension {extension_name}")
                except Exception as e:
                    logging.error(f"Failed to create extension tsm_system_rows: {e}")
                    return False
            else:
                logging.debug(f"Extension {extension_name} already exists")
        await async_engine.dispose()
        return True

    def _create_latest_version_db_uri(self):
        latest_version = max(self.client.materialize.get_versions())
        self.client.materialize.version = (
            latest_version  # make client set to latest version on error
        )
        sql_base_uri = self.default_uri.rpartition("/")[0]
        return make_url(f"{sql_base_uri}/{self.datastack_name}__mat{latest_version}")

    async def run(self, iterations=None, use_tsm_system_rows_extension=False):
        """
        Runs the Canary.

        Parameters:
            iterations (int, optional): The number of iterations to run. Defaults to `None`.
        """
        iteration_count = 0
        background_tasks = []
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if use_tsm_system_rows_extension:
            # check if postgresql extension is installed on database
            await self.check_if_extension_is_installed(extension_name="tsm_system_rows")

        annotation_tables = self.client.materialize.get_tables()
        version_info = self.client.materialize.get_version_metadata()

        while True:
            if iterations is not None and iteration_count >= iterations:
                break
            # check if event loop is running and add as task else run with eventloop
            if loop and loop.is_running():
                for table_name in annotation_tables:
                    task = loop.create_task(
                        self.check_random_annotations(table_name, version_info)
                    )
                    background_tasks.append(task)
                try:
                    await asyncio.gather(*background_tasks)
                except Exception as error:
                    logging.error(f"Error: {error}. Retrying task in next iteration.")

            background_tasks = []

            await asyncio.sleep(self.check_interval)

            iteration_count += 1

    async def check_random_annotations(self, table_name: str, version_info: dict):
        """
        Checks the specified random annotations.
        """
        async_engine = create_async_engine(self.database_uri)
        table_info = self.client.materialize.get_table_metadata(table_name)
        if not version_info["is_merged"]:
            if table_info.get("annotation_table"):
                table_name = f"{table_name}__{self.segmentation_source}"
            else:
                logging.debug(f"Skipping {table_name}, no segmentation data")
        return await self.query_data_and_check_roots(async_engine, table_name)

    async def query_data_and_check_roots(
        self,
        async_engine,
        table_name: str,
        sample_percent: int = 10,
        use_tsm_system_rows_extension: bool = False,
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
            sample_query = (
                f"SELECT * FROM {table_name} TABLESAMPLE SYSTEM_ROWS({self.num_test_annotations})"
                if use_tsm_system_rows_extension
                else f"SELECT * FROM {table_name} TABLESAMPLE system ({sample_percent}) WHERE random()<0.01 LIMIT {self.num_test_annotations}"
            )
            try:
                result = await conn.execute(text(sample_query))
            except Exception as e:
                raise (e)

            df = pd.DataFrame(result)

            if not df.empty:
                has_error = self.check_root_ids(df, table_name)
                logging.debug(f"TABLE NAME: {table_name}")
                logging.debug("USING SYSTEM_ROWS:{use_tsm_system_rows_extension}")
                logging.debug(df.id.describe())
                logging.debug(f"ERROR FOUND? {has_error}")
            else:
                has_error = False
        # close database connection
        await async_engine.dispose()
        return has_error

    def check_root_ids(self, df, table_name: str):
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
        root_id_columns = [col for col in df.columns if col.endswith("_root_id")]
        errors_found = False
        for supervoxel_col in supervoxel_columns:
            prefix = supervoxel_col[: -len("_supervoxel_id")]
            root_id_col = f"{prefix}_root_id"

            if root_id_col not in root_id_columns:
                continue

            try:
                timestamp = self.client.materialize.get_version_metadata()["time_stamp"]
                root_ids = self.client.chunkedgraph.get_roots(
                    df[supervoxel_col].values,
                    timestamp=timestamp,
                )
            except Exception as e:
                # self.send_slack_notification(f"Error in get_roots: {e}")
                logging.error(e)
                continue

            mismatch = df[root_id_col].values != root_ids

            if mismatch.any():
                bad_root_id_rows = df[mismatch]
                chunkgraph_root_id_values = root_ids[mismatch]
                formatted_bad_db_rows = bad_root_id_rows[["id", root_id_col]]
                mismatch_message = f"MISMATCHED DB ROWS:\n{formatted_bad_db_rows}\nVALID CHUNKGRAPH VALUES:\n{chunkgraph_root_id_values}"
                logging.debug(mismatch_message)

                slack_block_msg = [
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": "Mismatch Found"},
                    },
                    {"type": "divider"},
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f">Database: {self.database_uri.database} \n>Table: {table_name}\n>Lookup timestamp: {str(timestamp)}",
                        },
                    },
                    {"type": "divider"},
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": mismatch_message,
                        },
                    },
                ]

                self.send_slack_notification(message=None, blocks=slack_block_msg)
                errors_found = True
        return errors_found

    def send_slack_notification(self, message=None, blocks=None):
        """
        Sends a Slack notification with the specified message.

        Parameters:
            message (str): The message to send.
        """
        try:
            self.slack_client.chat_postMessage(
                channel=self.slack_channel, text=message, blocks=blocks
            )
        except SlackApiError as e:
            logging.error(f"Error sending Slack message: {e}")


if __name__ == "__main__":
    canary = Canary()
    loop = asyncio.new_event_loop()
    asyncio.get_event_loop().run_until_complete(canary.run())
    loop.close()
