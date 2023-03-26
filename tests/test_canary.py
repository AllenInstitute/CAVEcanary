import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np
from canary import Canary
import os

os.environ["CAVECANARY_CONFIG_FILE"] = "tests/test_config.cfg"


class TestCanary(unittest.TestCase):
    @patch("caveclient.CAVEclient")
    @patch("slack_sdk.WebClient")
    def setUp(self, mock_slack_client, mock_cave_client):
        
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
        self.canary = Canary(client=mock_cave_client)
        self.canary.client = mock_cave_client
        self.canary.slack_client = mock_slack_client

    def test_matching_root_ids(self):
        # Mock table metadata and DataFrame with matching root IDs
        self.canary.client.materialize.get_tables.return_value = ["test_table"]
        self.canary.client.materialize.get_annotation_count.return_value = 10
        data = {
            "pre_supervoxel_id": [1, 2, 3],
            "pre_rootid": [11, 22, 33],
            "post_supervoxel_id": [4, 5, 6],
            "post_rootid": [44, 55, 66],
        }
        df = pd.DataFrame(data)
        self.canary.client.materialize.query_table.return_value = df
        self.canary.client.chunkedgraph.get_roots.side_effect = (
            lambda x, **kwargs: np.array([11, 22, 33])
            if x[0] == 1
            else np.array([44, 55, 66])
        )

        self.canary.check_random_annotations()

        self.canary.slack_client.chat_postMessage.assert_not_called()

    def test_non_matching_root_ids(self):
        # Mock table metadata and DataFrame with non-matching root IDs
        self.canary.client.materialize.get_tables.return_value = ["test_table"]
        self.canary.client.materialize.get_annotation_count.return_value = 10
        data = {
            "pre_supervoxel_id": [1, 2, 3],
            "pre_rootid": [11, 22, 33],
            "post_supervoxel_id": [4, 5, 6],
            "post_rootid": [44, 55, 99],  # Non-matching root ID
        }
        df = pd.DataFrame(data)
        self.canary.client.materialize.query_table.return_value = df
        self.canary.client.chunkedgraph.get_roots.side_effect = (
            lambda x, **kwargs: np.array([11, 22, 33])
            if x[0] == 1
            else np.array([44, 55, 66])
        )

        self.canary.check_random_annotations()

        self.canary.slack_client.chat_postMessage.assert_called_once()

    def test_http_error(self):
        # Mock table metadata and simulate HTTP error
        self.canary.client.materialize.get_tables.return_value = ["test_table"]
        self.canary.client.materialize.get_annotation_count.return_value = 10
        self.canary.client.materialize.query_table.side_effect = Exception("HTTP Error")

        self.canary.check_random_annotations()

        self.canary.slack_client.chat_postMessage.assert_called_once()


if __name__ == "__main__":
    unittest.main()
