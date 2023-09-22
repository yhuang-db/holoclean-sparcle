import logging

import pandas as pd
import polars as pl

from utils import NULL_REPR
from .detector import Detector


class SDCDetector(Detector):
    def __init__(self, name='SDCDetector'):
        super(SDCDetector, self).__init__(name)

    def setup(self, dataset, env):
        self.ds = dataset
        self.env = env
        self.sdc = self.ds.sparcle_constraints

    def detect_noisy_cells(self):
        """
        Returns a pandas.DataFrame containing all cells that
        shows conflict to spatial denial constraints in self.dataset.

        :return: pandas.DataFrame with columns:
            _tid_: entity ID
            attribute: tobler_attr

        Pair of cells conflicting spatial denial constraints:
        Two cells within proximity threshold has different value
        """
        error_dfs = []
        for sdc in self.sdc:
            pdf = pl.read_database(sdc.get_error_sql(), self.ds.engine.polars_conn)
            error_dict = {'_tid_': pdf['tid_1'].to_list()}
            df_error = pd.DataFrame(error_dict)
            df_error['attribute'] = sdc.attr
            error_dfs.append(df_error)
            logging.debug(f"SPARCLE: detect {len(df_error)} errors by {sdc}")

        df_error = None
        if error_dfs:
            df_error = pd.concat(error_dfs, ignore_index=True)
            if df_error.shape[0]:
                df_error = df_error.drop_duplicates().reset_index(drop=True)
        logging.debug(f"SPARCLE: SDCDetector: detect {len(df_error)} errors")
        return df_error
