import configparser
import datetime
import pathlib
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from canary import Canary


@pytest.fixture
def mock_caveclient():
    mock_caveclient = MagicMock()
    mock_caveclient.materialize.get_tables.return_value = ["example_table"]
    mock_caveclient.materialize.get_version_metadata.return_value = {"is_merged": False}
    mock_caveclient.materialize.materialize.get_versions.return_value = 1
    mock_caveclient.materialize.get_table_metadata.return_value = {
        "annotation_table": True,
        "time_stamp": str(datetime.datetime.utcnow()),
    }
    mock_caveclient.materialize.get_annotation_count.return_value = 100
    mock_caveclient.materialize.query_table.return_value = pd.DataFrame()
    mock_caveclient.chunkedgraph.get_roots.return_value = np.array([100, 200, 300])
    mock_caveclient.info.get_datastack_info.return_value = {
        "segmentation_source": "example_segmentation_source"
    }
    yield mock_caveclient


@pytest.fixture
def mock_pd_dataframe():
    # Create a mock dataframe with some test data
    data = {
        "id": [1, 2, 3],
        "pt_supervoxel_id": [1, 2, 3],
        "pt_root_id": [100, 200, 400],
    }
    df = pd.DataFrame(data)
    # Return a MagicMock object that wraps the dataframe
    return MagicMock(wraps=df)


@pytest.fixture
def mock_slack_client():
    mock_slack_client = MagicMock()
    mock_slack_client.chat_postMessage.return_value = None
    return mock_slack_client


@pytest.fixture
def canary_instance(mock_caveclient, mock_slack_client):
    config_path = pathlib.Path(__file__).parent.absolute() / "test_config.cfg"
    config = configparser.ConfigParser()
    config.read(config_path)
    return Canary(client=mock_caveclient, config=config, slack_client=mock_slack_client)


@pytest.fixture
def mock_async_engine():
    mock_async_engine = MagicMock()
    mock_async_engine.begin.return_value.__aenter__.return_value = mock_async_engine
    return mock_async_engine


def test_canary_init(mock_caveclient, mock_slack_client):
    config_path = pathlib.Path(__file__).parent.absolute() / "test_config.cfg"
    config = configparser.ConfigParser()
    config.read(config_path)
    canary = Canary(
        client=mock_caveclient, config=config, slack_client=mock_slack_client
    )

    assert canary.client == mock_caveclient
    assert canary.slack_client == mock_slack_client
    assert canary.config == config
    assert canary.database_uri == config["SETTINGS"]["DATABASE_URI"]
    assert canary.datastack_name == config["SETTINGS"]["DATASTACK_NAME"]
    assert canary.server_address == config["SETTINGS"]["SERVER_ADDRESS"]
    assert canary.slack_api_token == config["SETTINGS"]["SLACK_API_TOKEN"]
    assert canary.slack_channel == config["SETTINGS"]["SLACK_CHANNEL"]


@pytest.mark.asyncio
async def test_check_random_annotations(
    canary_instance, mock_caveclient, mock_async_engine
):
    # Configure the mocked CAVEclient and async engine behavior
    mock_caveclient.materialize.get_tables.return_value = ["example_table"]
    mock_caveclient.materialize.get_version_metadata.return_value = {"is_merged": False}
    mock_caveclient.materialize.get_table_metadata.return_value = {
        "annotation_table": True
    }
    canary_instance.query_data_and_check_roots = AsyncMock(return_value=False)
    # Patch the create_async_engine function to return a new mocked async engine
    with patch(
        "sqlalchemy.ext.asyncio.create_async_engine"
    ) as mock_create_async_engine:
        mock_async_engine = MagicMock(
            spec=create_async_engine(canary_instance.database_uri)
        )
        mock_create_async_engine.return_value = mock_async_engine

        # Run the method and capture the result
        result = await canary_instance.check_random_annotations()

    # Check if the method returned the expected result
    assert result is False

    # Check if the query_data_and_check_roots method was called with the expected arguments
    expected_async_engine = mock_create_async_engine.return_value
    (
        actual_async_engine,
        actual_table_name,
    ) = canary_instance.query_data_and_check_roots.call_args[0]

    assert actual_table_name == f"example_table__{canary_instance.segmentation_source}"


def test_check_root_ids(canary_instance, mock_caveclient):
    # Mock the CAVEclient behavior
    mock_caveclient.chunkedgraph.get_roots.return_value = np.array([100, 200, 300])

    # Patch the get_version_metadata method to return a fixed timestamp
    with patch.object(
        canary_instance.client.materialize, "get_version_metadata"
    ) as mock_get_version_metadata:
        mock_get_version_metadata.return_value = {"time_stamp": 1234567890}

        # Case 1: Mismatch in root IDs
        data = {
            "pt_supervoxel_id": [1, 2, 3],
            "pt_root_id": [100, 200, 400],
        }
        df1 = pd.DataFrame(data)

        # Mock the send_slack_notification method to prevent actual Slack messages
        canary_instance.send_slack_notification = MagicMock()

        result1 = canary_instance.check_root_ids(df1)

        assert result1 is True
        canary_instance.send_slack_notification.assert_called_once()

        # Reset the mock for the next case
        canary_instance.send_slack_notification.reset_mock()

        # Case 2: No mismatch in root IDs
        data = {
            "pt_supervoxel_id": [1, 2, 3],
            "pt_root_id": [100, 200, 300],
        }
        df2 = pd.DataFrame(data)

        result2 = canary_instance.check_root_ids(df2)

        assert result2 is False
        canary_instance.send_slack_notification.assert_not_called()


@pytest.mark.asyncio
async def test_canary_run(canary_instance):
    # Mock the check_random_annotations method to prevent actual execution
    canary_instance.check_random_annotations = AsyncMock(return_value=None)

    # Run the method with a single iteration for testing purposes
    await canary_instance.run(iterations=1)

    # Check if the check_random_annotations method was called once
    canary_instance.check_random_annotations.assert_called_once()
