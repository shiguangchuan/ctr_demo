#!/usr/bin/env python
#coding=utf-8


import os
import sys

from dango import Data
from dango import Plan
from dango import Task


def run_task():
    i_d = Data.query('output_data')
    o_d = Data('output_model')

    model_train = Task.create('ModelTraining')
    model_train.inputs = [i_d]
    model_train.outputs = [o_d]

    # configure
    model_train.set_conf('training_bits', 10)
    model_train.run()

def run_plan():
    task_list = []
    app_schema = ['app_id', 'app_category']
    label_schema = ['show_id', 'clk_time']
    show_schema = ['show_id', 'show_time', 'C1', 'banner_pos', 'site_id', 'site_domain', 'app_id', 'app_domain', 'device_id', 'device_ip', 'device_model', 'device_type', 'device_conn_type', 'C14', 'C15', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21']
    site_schema = ['site_id', 'site_category']
    
    input_root = './data'

    # import input table
    input_app   = Data.create('ctr_demo_input_app',   'file://%s/app'%input_root,   app_schema)
    input_site  = Data.create('ctr_demo_input_site',  'file://%s/site'%input_root,  site_schema)
    input_label = Data.create('ctr_demo_input_label', 'file://%s/label'%input_root, label_schema)
    input_show  = Data.create('ctr_demo_input_show',  'file://%s/show'%input_root,  show_schema)
    #input_app = Data.query('ctr_demo_input_app')
    #input_site = Data.query('ctr_demo_input_site')
    #input_label = Data.query('ctr_demo_input_label')
    #input_show = Data.query('ctr_demo_input_show')
    #input_table3 = Data.create('data_name3', 'hdfs://master.tfp.com:8000/hdfs_path', data_schema3)

    # define output table
    output_streaming_time_norm_label = Data.create('ctr_demo_streaming_time_norm_label')
    output_streaming_time_norm_show = Data.create('ctr_demo_streaming_time_norm_show')
    output_join_show_app = Data.create('ctr_demo_join_show_app')
    output_join_show_app_site = Data.create('ctr_demo_join_show_app_site')
    output_join_show_app_site_label = Data.create('ctr_demo_join_show_app_site_label')
    output_streaming_rename_label = Data.create('ctr_demo_streaming_rename_label')
    output_streaming_split_train = Data.create('ctr_demo_streaming_split_train')
    output_streaming_split_eval = Data.create('ctr_demo_streaming_split_eval')
    output_feature_extract_train = Data.create('ctr_demo_feature_extract_train')
    output_feature_extract_eval = Data.create('ctr_demo_feature_extract_eval')
    output_shuffle_train = Data.create('ctr_demo_streaming_shuffle_train', type=Data.Instance)
    output_train = Data.create('ctr_demo_output_model')
    output_eval = Data.create('ctr_demo_output_eval')

    # time normalize, datetime => hour
    streaming_time_norm_label = Task.create('xxxxxxxxx', name='streaming_time_norm_label')
    streaming_time_norm_label.upstream = [xxxxxxxx(input_label)]
    output_streaming_time_norm_label.schema = label_schema
    streaming_time_norm_label.outputs = [output_streaming_time_norm_label]
    streaming_time_norm_label.set_conf(xxxxxxxxx, "./conf/streaming_time_norm.yaml")
    task_list.append(streaming_time_norm_label)

    streaming_time_norm_show = Task.create('xxxxxxxxx', name='streaming_time_norm_show')
    streaming_time_norm_show.upstream = [xxxxxxxxxxxxx(input_show)]
    output_streaming_time_norm_show.schema = show_schema
    streaming_time_norm_show.outputs = [output_streaming_time_norm_show]
    streaming_time_norm_show.set_conf(xxxxxxxxxxxxxxxxxxx, './conf/streaming_time_norm.yaml')
    task_list.append(streaming_time_norm_show)

    # JOIN
    join_show_app = Task.create('TableJoin', name='join_show_app')
    join_show_app.upstream = [streaming_time_norm_show, xxxxxxxxxxxxx(input_app)]
    join_show_app.outputs = [output_join_show_app]
    join_show_app.set_conf('xxxxxxxxxxx', './conf/join_show_app.yaml')
    task_list.append(join_show_app)

    join_show_app_site = Task.create('TableJoin', name='join_show_app_site')
    join_show_app_site.upstream = [join_show_app, xxxxxxxxxxxxx(input_site)]
    join_show_app_site.outputs = [output_join_show_app_site]
    join_show_app_site.set_conf('xxxxxxxxxxx', './conf/join_show_app_site.yaml')
    task_list.append(join_show_app_site)

    join_show_app_site_label = Task.create('TableJoin', name='join_show_app_site_label')
    join_show_app_site_label.upstream = [join_show_app_site, streaming_time_norm_label]
    join_show_app_site_label.outputs = [output_join_show_app_site_label]
    join_show_app_site_label.set_conf('xxxxxxxxxxx', './conf/join_show_app_site_label.yaml')
    task_list.append(join_show_app_site_label)

    # rename label, >0 => 1, =0 or empty => -1
    streaming_rename_label = Task.create('xxxxxxxxx', name='streaming_rename_label')
    streaming_rename_label.upstream = [join_show_app_site_label]
    output_streaming_rename_label.schema = join_show_app_site_label.outputs()[0].schema()
    streaming_rename_label.outputs = [output_streaming_rename_label]
    streaming_rename_label.set_conf(xxxxxxxxxxxxxxxxxxxx, './conf/streaming_rename_label.yaml')
    task_list.append(streaming_rename_label)

    # split training & eval
    streaming_split_train = Task.create('xxxxxxxxx', name='streaming_split_train')
    streaming_split_train.upstream = [streaming_rename_label]
    output_streaming_split_train.schema = streaming_split_train.inputs()[0].schema()
    streaming_split_train.outputs = [output_streaming_split_train]
    streaming_split_train.set_conf(xxxxxxxxxxxxxxxxxxxx, './conf/streaming_split_train.yaml')
    task_list.append(streaming_split_train)

    streaming_split_eval = task.create('xxxxxxxxx', name='streaming_split_eval')
    streaming_split_eval.upstream = [streaming_rename_label]
    output_streaming_split_eval.schema = streaming_split_eval.inputs()[0].schema()
    streaming_split_eval.outputs = [output_streaming_split_eval]
    streaming_split_eval.set_conf(xxxxxxxxxxxxxxxxxxxx, './conf/streaming_split_eval.yaml')
    task_list.append(streaming_split_eval)

    # feature extract
    fe_train = Task.create('FeatureExtraction', name='feture_extract_train')
    fe_train.set_conf('extraction_conf', './conf/feature_list.config')
    fe_train.upstream = [streaming_split_train]
    fe_train.outputs = [output_feature_extract_train]
    task_list.append(fe_train)

    fe_eval = Task.create('FeatureExtraction', name='feture_extract_eval')
    fe_eval.set_conf('extraction_conf', './conf/feature_list.config')
    fe_eval.upstream = [streaming_split_eval]
    fe_eval.outputs = [output_feature_extract_eval]
    task_list.append(fe_eval)

    # shuffle training instance
    streaming_shuffle_train = Task.create('xxxxxxxxxxxxxxxxxxxx', name='streaming_shuffle_train')
    streaming_shuffle_train.upstream = [fe_train]
    streaming_shuffle_train.outputs = [output_shuffle_train]
    streaming_shuffle_train.set_conf(xxxxxxxxxxxxxxxx, './conf/streaming_shuffle.yaml')
    task_list.append(streaming_shuffle_train)

    # model train
    model_train = Task.create('ModelTraining', name='model_train')
    model_train.outputs = [output_train]
    model_train.set_conf('training_bits', 10)
    model_train.upstream = [streaming_shuffle_train]
    task_list.append(model_train)

    # model eval
    model_eval = Task.create('xxxxxxxxxxxxxxxx', name='model_evaluate')
    model_eval.outputs([output_eval])
    model_eval.set_conf(xxxxxxxxxxxxxxxxxxx, xxxxxxxxxxxxxxxxxxx)
    model_eval.upstream([model_train, fe_eval])
    task_list.append(model_eval)

    plan = Plan.create('ctr_demo')
    plan.pipeline(task_list)
    plan.run()
    print 'Plan ID=%s is running' % plan.id
    plan.wait()
    print 'Plan finished, status=%s' % plan.status

def resume_plan(plan_id):
    plan = Plan.get_plan(plan_id)
    plan.resume()
    plan.wait()
    plan.poll()

if __name__ == '__main__':
    run_plan()
    #resume_plan(plan_id)

