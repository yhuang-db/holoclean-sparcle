import argparse
import time

import toml

import holoclean
from dcparser.sdcparser import *
from detect import *
from eval_driver import attr_evaluation
from repair.featurize import *

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--DBName")
    parser.add_argument("-t", "--Toml")
    parser.add_argument("-p", "--Path")
    parser.add_argument("-k", "--Knn")
    parser.add_argument("-r", "--Distance")
    parser.add_argument("-n", "--Weight")
    args = parser.parse_args()
    db_name = args.DBName
    toml_file = args.Toml
    data_dir = args.Path
    args_knn = args.Knn
    args_distance = args.Distance
    args_weight = args.Weight

    # toml config
    print(f'Experiment configure file: {toml_file}')
    toml_dict = toml.load(toml_file)

    # dataset
    dataset_config = toml_dict['dataset']
    dataset_city = dataset_config['city']
    dataset_name = dataset_config['name']
    raw_file_name = dataset_config['raw_file_name']
    clean_file_name = dataset_config['clean_file_name']
    raw_fpath = f'{data_dir}/{raw_file_name}'
    clean_fpath = f'{data_dir}/{clean_file_name}'
    print(f'Raw data path: {raw_fpath}')
    print(f'Clean data path: {clean_fpath}')

    # constraints
    constraint_config = toml_dict['constraint']
    dc_path = constraint_config['dc_path']
    knn_sdc = constraint_config['knn']
    distance_sdc = constraint_config['distance']
    print(f'DC path: {dc_path}')

    if args_knn is not None and args_weight is not None:
        knn_sdc_list = [KnnSDC(knn[0], knn[1], knn[2], k=int(args_knn), weight_function_factor_n=int(args_weight)) for knn in knn_sdc]
        toml_dict['evaluation']['file_name'] = f'{dataset_name}_k{args_knn}n{args_weight}.csv'
    else:
        knn_sdc_list = [KnnSDC(knn[0], knn[1], knn[2], k=int(knn[3]), weight_function_factor_n=int(knn[4])) for knn in knn_sdc]
    if args_distance is not None and args_weight is not None:
        distance_sdc_list = [DistanceSDC(distance[0], distance[1], distance[2], d=int(args_distance), weight_function_factor_n=int(args_weight)) for distance in
                             distance_sdc]
        toml_dict['evaluation']['file_name'] = f'{dataset_name}_d{args_distance}n{args_weight}.csv'
    else:
        distance_sdc_list = [DistanceSDC(distance[0], distance[1], distance[2], d=int(distance[3]), weight_function_factor_n=int(distance[4])) for distance in
                             distance_sdc]
    print(f'KNN SDC: {knn_sdc_list}')
    print(f'Distance SDC: {distance_sdc_list}')

    # parameters
    parameter_config = toml_dict['parameter']
    epochs = parameter_config['epochs']
    estimator_type = parameter_config['estimator_type']
    bayes_label_threshold = parameter_config['bayes_label_threshold']
    bayes_drop_threshold = parameter_config['bayes_drop_threshold']
    print(f'Epochs: {epochs}')
    print(f'Estimator type: {estimator_type}')
    print(f'Bayes label threshold: {bayes_label_threshold}')
    print(f'Bayes drop threshold: {bayes_drop_threshold}')

    # detector
    detector_config = toml_dict['detector']
    null_detector = detector_config['null']
    sdc_detector = detector_config['sparcle_dc']
    violation_detector = detector_config['dc']
    print(f'Null detector: {null_detector}')
    print(f'SDC detector: {sdc_detector}')
    print(f'Violation detector: {violation_detector}')

    # estimator
    estimator_config = toml_dict['estimator']
    run_estimator = estimator_config['run_estimator']

    # feature
    feature_config = toml_dict['feature']
    continuous_feature = feature_config['continuous']
    constraint_feature = feature_config['constraint']
    print(f'Continuous feature: {continuous_feature}')
    print(f'Constraint feature: {constraint_feature}')

    # start timer
    start_time = time.time()

    # 1. Setup a HoloClean session.
    hc = holoclean.HoloClean(
        db_name=db_name,
        domain_thresh_1=0.0,
        domain_thresh_2=bayes_drop_threshold,
        weak_label_thresh=bayes_label_threshold,
        max_domain=10000,
        cor_strength=0.6,
        nb_cor_strength=0.8,
        verbose=True,
        timeout=5 * 60000,
        print_fw=True,
        # tobler env
        epochs=epochs,
        sparcle_dc=knn_sdc_list + distance_sdc_list,
        estimator_type=estimator_type,
    ).session

    # 2. Load training data and denial constraints.
    hc.load_data(dataset_name, raw_fpath)
    hc.load_dcs(dc_path)
    hc.ds.set_constraints(hc.get_dcs())
    hc.ds.set_sparcle_constraints(hc.env['sparcle_dc'])
    hc.ds.setup_sparcle_constraints()

    # 3. Detect erroneous cells using these two detectors.
    detectors = []
    if null_detector:
        detectors.append(NullDetector())
    if sdc_detector:
        detectors.append(SDCDetector())
    if violation_detector:
        detectors.append(ViolationDetector())
    hc.detect_errors(detectors)

    # 4. Repair errors utilizing the defined features.
    hc.generate_domain()
    if run_estimator:
        hc.run_estimator()

    featurizers = []
    if continuous_feature:
        featurizers.append(ContinuousFeaturizer())
    if constraint_feature:
        featurizers.append(ConstraintFeaturizer())
    hc.repair_errors(featurizers)

    # end timer
    end_time = time.time()
    # print time in minutes and seconds
    runtime_str = f'{(end_time - start_time) / 60:.0f}m{(end_time - start_time) % 60:.0f}s'
    print(f'Runtime: {runtime_str}')

    # 5. Evaluate the correctness of the results.
    report = hc.evaluate(fpath=clean_fpath,
                         tid_col='tid',
                         attr_col='attribute',
                         val_col='correct_val')

    # 6. Attribute evaluation
    if 'evaluation' in toml_dict:
        attr_evaluation(toml_dict, dataset_city, dataset_name, runtime_str)
