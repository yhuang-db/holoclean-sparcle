import logging
import math

import pandas as pd
from tqdm import tqdm

from utils import NULL_REPR
from ..estimators.naive_bayes import NaiveBayes


class SpatialBayes(NaiveBayes):
    def __init__(self, env, dataset, domain_df, correlations):
        NaiveBayes.__init__(self, env, dataset, domain_df, correlations)

        self.sparcle_constraints = self.ds.sparcle_constraints
        self.spatial_attr_set = set()
        self.attr_precounting_dist = {}

        for sdc in self.sparcle_constraints:
            self.spatial_attr_set.update([sdc.x, sdc.y])

            logging.debug(f'SPARCLE: preparing counting dict for sdc {sdc}')
            df = pd.read_sql(sdc.get_counting_sql(), self.ds.engine.polars_conn)
            precounting_dict = {}
            for (_, tid_1, val_2, counting) in tqdm(df.itertuples()):
                # for row in tqdm(df.to_records()):
                if tid_1 not in precounting_dict:
                    precounting_dict[tid_1] = {val_2: counting}
                else:
                    precounting_dict[tid_1][val_2] = counting

            attr = sdc.attr
            if attr in self.attr_precounting_dist:
                self.attr_precounting_dist[attr].append(precounting_dict)
            else:
                self.attr_precounting_dist[attr] = [precounting_dict]

    def _predict_pp(self, row, attr, values):
        tid = row["_tid_"]

        nb_score = []
        correlated_attributes = self._get_corr_attributes(attr)
        non_spatial_correlated_attributes = []
        for i in correlated_attributes:
            if i not in self.spatial_attr_set:
                non_spatial_correlated_attributes.append(i)

        for val1 in values:
            val1_count = self._freq[attr][val1]
            log_prob = math.log(float(val1_count) / float(self._n_tuples))

            for at in non_spatial_correlated_attributes:
                # Ignore same attribute, index, and tuple id.
                if at == attr or at == '_tid_':
                    continue
                val2 = row[at]
                # Since we do not have co-occurrence stats with NULL values,
                # we skip them.
                # It also doesn't make sense for our likelihood to be conditioned
                # on a NULL value.
                if val2 == NULL_REPR:
                    continue
                val2_val1_count = 0.1
                if val1 in self._cooccur_freq[attr][at]:
                    if val2 in self._cooccur_freq[attr][at][val1]:
                        val2_val1_count = max(self._cooccur_freq[attr][at][val1][val2] - 1.0, 0.1)
                p = float(val2_val1_count) / float(val1_count)
                log_prob += math.log(p)

            for pre_counting_dict in self.attr_precounting_dist[attr]:
                xy_val1_count = 0.1
                if tid in pre_counting_dict:
                    if val1 in pre_counting_dict[tid]:
                        xy_val1_count = pre_counting_dict[tid][val1]

                p = float(xy_val1_count) / float(val1_count)
                log_prob += math.log(p)
            nb_score.append((val1, log_prob))
        denom = sum(map(math.exp, [log_prob for _, log_prob in nb_score]))

        def val_probas():
            for val, log_prob in nb_score:
                yield val, math.exp(log_prob) / denom

        return val_probas()
