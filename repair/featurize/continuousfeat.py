import logging
from string import Template

import torch
import torch.nn.functional as F

from dataset import AuxTables
from .featurizer import Featurizer

# checked, correct
template_from_distance_matrix_lite = Template(
    '''
    SELECT t1._vid_, t1.val_id, SUM(t2.weight) AS weights
      FROM $pos_values t1, $distance_matrix t2
     WHERE t1.attribute = \'$sdc_attr\'
       AND t1._tid_ = t2.tid_1
       AND t1.rv_val::TEXT <> t2.val_2
    GROUP BY _vid_, val_id;
    '''
)


def gen_feat_tensor(violations, total_vars, classes):
    tensor = torch.zeros(total_vars, classes, 1)
    if violations:
        for entry in violations:
            vid = int(entry[0])
            val_id = int(entry[1]) - 1
            feat_val = float(entry[2])
            tensor[vid][val_id][0] = feat_val
    return tensor


class ContinuousFeaturizer(Featurizer):

    def specific_setup(self):
        self.name = "SPARCLEFeaturizer"
        self.sdc = self.ds.sparcle_constraints

    def create_tensor(self):
        """
        This method creates a tensor which has shape
        (# of cells/rvs, max size of domain, # of features for this featurizer)
        :return: PyTorch Tensor
        """
        tensors = []
        for sdc in self.sdc:
            query = template_from_distance_matrix_lite.substitute(
                pos_values=AuxTables.pos_values.name,
                distance_matrix=sdc.distance_matrix_table_name,
                sdc_attr=sdc.attr
            )
            logging.debug(f'SPARCLE: ContinuousFeaturizer: start execute {query}')
            result = self.ds.engine.execute_query(query)
            logging.debug('SPARCLE: ContinuousFeaturizer: finish query')
            weighted_violations = [[i[0], i[1], i[2]] for i in result]
            tensor = gen_feat_tensor(weighted_violations, self.total_vars, self.classes)
            tensors.append(tensor)
        combined = torch.cat(tensors, 2)
        combined = F.normalize(combined, p=2, dim=1)
        return combined

    def feature_names(self):
        return [f'SPARCLE Feature : {sdc}' for sdc in self.sdc]
