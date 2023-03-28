import asyncio
import os

import numpy as np
import pandas as pd
import pytest
from canary import Canary
from caveclient import CAVEclient
from slack_sdk import WebClient
from unittest.mock import patch

os.environ["CAVECANARY_CONFIG_FILE"] = "tests/test_config.cfg"


@pytest.fixture
def mock_canary():
    with patch("caveclient.CAVEclient") as mock_cave_client, patch(
        "slack_sdk.WebClient"
    ) as mock_slack_client:
        # Mock client.info.get_datastack_info with test values
        mock_datastack_info = {
            "soma_table": "test_nucleus_neuron_svm",
            "description": "Test datastack description.",
            "aligned_volume": {
                "image_source": "precomputed://https://test-bossdb-open-data.s3.amazonaws.com/test/minnie/em",
                "description": "Test aligned volume description.",
                "id": 1,
                "name": "test_minnie65_phase3",
            },
            "viewer_site": "https://test-neuroglancer.neuvue.io/",
            "analysis_database": None,
            "viewer_resolution_y": 4.0,
            "viewer_resolution_z": 40.0,
            "synapse_table": "test_synapses_pni_2",
            "local_server": "https://test.minnie.microns-daf.com",
            "viewer_resolution_x": 4.0,
            "segmentation_source": "graphene://https://test.minnie.microns-daf.com/segmentation/table/test_minnie3_v1",
        }

        mock_cave_client.info.get_datastack_info.return_value = mock_datastack_info
        canary = Canary(client=mock_cave_client)
        canary.client = mock_cave_client
        canary.slack_client = mock_slack_client

        yield canary


@pytest.mark.asyncio
async def test_matching_root_ids(mock_canary):
    # Mock table metadata and DataFrame with matching root IDs
    mock_canary.client.materialize.get_tables.return_value = ["test_table"]
    mock_canary.client.materialize.get_annotation_count.return_value = 10
    data = {
        "pre_supervoxel_id": [1, 2, 3],
        "pre_rootid": [11, 22, 33],
        "post_supervoxel_id": [4, 5, 6],
        "post_rootid": [44, 55, 66],
    }
    df = pd.DataFrame(data)
    mock_canary.client.materialize.query_table.return_value = df
    mock_canary.client.chunkedgraph.get_roots.side_effect = (
        lambda x, **kwargs: np.array([11, 22, 33])
        if x[0] == 1
        else np.array([44, 55, 66])
    )

    await mock_canary.check_random_annotations()

    mock_canary.slack_client.chat_postMessage.assert_not_called()


@pytest.mark.asyncio
async def test_non_matching_root_ids(mock_canary):
    # Mock table metadata and DataFrame with non-matching root IDs
    mock_canary.client.materialize.get_tables.return_value = ["test_table"]
    mock_canary.client.materialize.get_annotation_count.return_value = 10
    data = {
        "pre_supervoxel_id": [1, 2, 3],
        "pre_rootid": [11, 22, 33],
        "post_supervoxel_id": [4, 5, 6],
        "post_rootid": [44, 55, 99],  # Non-matching root ID
    }
    df = pd.DataFrame(data)
    mock_canary.client.materialize.query_table.return_value = df
    mock_canary.client.chunkedgraph.get_roots.side_effect = (
        lambda x, **kwargs: np.array([11, 22, 33])
        if x[0] == 1
        else np.array([44, 55, 66])
    )
    await mock_canary.check_random_annotations()

    mock_canary.slack_client.chat_postMessage.assert_called_once()


@pytest.mark.asyncio
async def test_http_error(mock_canary):
    # Mock table metadata and simulate HTTP error
    mock_canary.client.materialize.get_tables.return_value = ["test_table"]
    mock_canary.client.materialize.get_annotation_count.return_value = 10
    mock_canary.client.materialize.query_table.side_effect = Exception("HTTP Error")

    await mock_canary.check_random_annotations()

    mock_canary.slack_client.chat_postMessage.assert_called_once()
