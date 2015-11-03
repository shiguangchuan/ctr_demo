#!/usr/bin/env python
#coding=utf-8


import os
import sys

from dango.data import *
from dango.plan import *
from dango.task import *



def run_plan():
    task_list = []
    app_schema = ['app_id', 'app_category']
    label_schema = ['show_id', 'clk_time']
    show_schema = ['show_id', 'show_time', 'C1', 'banner_pos', 'site_id', 'site_domain', 'app_id', 'app_domain', 'device_id', 'device_ip', 'device_model', 'device_type', 'device_conn_type', 'C14', 'C15', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21']
    site_schema = ['site_id', 'site_category']
    
    input_root = './data'
    conf_root = './working_conf'

    # import input table
    input_app = Data.create('ctr_demo_input_app', uri='file://%s/app'%input_root, schema=app_schema) if not Data.exists('ctr_demo_input_app') else Data.query('ctr_demo_input_app')
    input_site = Data.create('ctr_demo_input_site', uri='file://%s/site'%input_root, schema=site_schema) if not Data.exists('ctr_demo_input_site') else Data.query('ctr_demo_input_site')
    input_label = Data.create('ctr_demo_input_label', uri='file://%s/label'%input_root, schema=label_schema) if not Data.exists('ctr_demo_input_label') else Data.query('ctr_demo_input_label')
    input_show = Data.create('ctr_demo_input_show', uri='file://%s/show'%input_root, schema=show_schema) if not Data.exists('ctr_demo_input_show') else Data.query('ctr_demo_input_show')
    
    # define output table
    output_streaming_time_norm_label = Data.create('ctr_demo_streaming_time_norm_label') if not Data.exists('ctr_demo_streaming_time_norm_label') else Data.query('ctr_demo_streaming_time_norm_label')
    output_streaming_time_norm_show = Data.create('ctr_demo_streaming_time_norm_show') if not Data.exists('ctr_demo_streaming_time_norm_show') else Data.query('ctr_demo_streaming_time_norm_show')
    output_streaming_time_filter_show = Data.create('ctr_demo_streaming_time_filter_show') if not Data.exists('ctr_demo_streaming_time_filter_show') else Data.query('ctr_demo_streaming_time_filter_show')
    output_join_show_app = Data.create('ctr_demo_join_show_app') if not Data.exists('ctr_demo_join_show_app') else Data.query('ctr_demo_join_show_app')
    output_join_show_app_site = Data.create('ctr_demo_join_show_app_site') if not Data.exists('ctr_demo_join_show_app_site') else Data.query('ctr_demo_join_show_app_site')
    output_join_show_app_site_label = Data.create('ctr_demo_join_show_app_site_label') if not Data.exists('ctr_demo_join_show_app_site_label') else Data.query('ctr_demo_join_show_app_site_label')
    output_streaming_rename_label = Data.create('ctr_demo_streaming_rename_label') if not Data.exists('ctr_demo_streaming_rename_label') else Data.query('ctr_demo_streaming_rename_label')
    output_streaming_split_train = Data.create('ctr_demo_streaming_split_train') if not Data.exists('ctr_demo_streaming_split_train') else Data.query('ctr_demo_streaming_split_train')
    output_streaming_split_eval = Data.create('ctr_demo_streaming_split_eval') if not Data.exists('ctr_demo_streaming_split_eval') else Data.query('ctr_demo_streaming_split_eval')
    output_feature_extract_train = Data.create('ctr_demo_feature_extract_train') if not Data.exists('ctr_demo_feature_extract_train') else Data.query('ctr_demo_feature_extract_train')
    output_feature_extract_eval = Data.create('ctr_demo_feature_extract_eval') if not Data.exists('ctr_demo_feature_extract_eval') else Data.query('ctr_demo_feature_extract_eval')
    output_shuffle_train = Data.create('ctr_demo_streaming_shuffle_train', type=Data.Instance) if not Data.exists('ctr_demo_streaming_shuffle_train') else Data.query('ctr_demo_streaming_shuffle_train')
    output_train = Data.create('ctr_demo_output_model') if not Data.exists('ctr_demo_output_model') else Data.query('ctr_demo_output_model')
    output_eval = Data.create('ctr_demo_output_eval') if not Data.exists('ctr_demo_output_eval') else Data.query('ctr_demo_output_eval')
    
    #input_table3 = Data.create('data_name3', uri='hdfs://master.tfp.com:8000/hdfs_path', schema=data_schema3)


    # time normalize, datetime => hour
    streaming_time_norm_label = Task.create('HadoopStreaming', name='streaming_time_norm_label')
    streaming_time_norm_label.upstream = [Task.create(datas=[input_label])]
    output_streaming_time_norm_label.schema = label_schema
    streaming_time_norm_label.outputs = [output_streaming_time_norm_label]
    streaming_time_norm_label.set_conf('conf', '%s/streaming_time_norm.yaml'%conf_root)
    streaming_time_norm_label.solid()
    task_list.append(streaming_time_norm_label)

    streaming_time_norm_show = Task.create('HadoopStreaming', name='streaming_time_norm_show')
    streaming_time_norm_show.upstream = [Task.create(datas=[input_show])]
    output_streaming_time_norm_show.schema = show_schema
    streaming_time_norm_show.outputs = [output_streaming_time_norm_show]
    streaming_time_norm_show.set_conf('conf', '%s/streaming_time_norm.yaml'%conf_root)
    streaming_time_norm_show.solid()
    task_list.append(streaming_time_norm_show)

    # filter show data between 10.21 ~ 10.30
    streaming_time_filter_show = Task.create('HadoopStreaming', name='streaming_time_filter_show')
    streaming_time_filter_show.upstream = [streaming_time_norm_show]
    output_streaming_time_filter_show.schema = show_schema
    streaming_time_filter_show.outputs = [output_streaming_time_filter_show]
    streaming_time_filter_show.set_conf('conf', '%s/streaming_time_filter_show.yaml'%conf_root)
    streaming_time_filter_show.solid()
    task_list.append(streaming_time_filter_show)

    # JOIN
    join_show_app = Task.create('TableJoint', name='join_show_app')
    join_show_app.upstream = [streaming_time_filter_show, Task.create(datas=[input_app])]
    join_show_app.outputs = [output_join_show_app]
    join_show_app.set_conf('conf', '%s/join_show_app.yaml'%conf_root)
    join_show_app.solid()
    task_list.append(join_show_app)

    join_show_app_site = Task.create('TableJoint', name='join_show_app_site')
    join_show_app_site.upstream = [join_show_app, Task.create(datas=[input_site])]
    join_show_app_site.outputs = [output_join_show_app_site]
    join_show_app_site.set_conf('conf', '%s/join_show_app_site.yaml'%conf_root)
    join_show_app_site.solid()
    task_list.append(join_show_app_site)

    join_show_app_site_label = Task.create('TableJoint', name='join_show_app_site_label')
    join_show_app_site_label.upstream = [join_show_app_site, streaming_time_norm_label]
    join_show_app_site_label.outputs = [output_join_show_app_site_label]
    join_show_app_site_label.set_conf('conf', '%s/join_show_app_site_label.yaml'%conf_root)
    join_show_app_site_label.solid()
    task_list.append(join_show_app_site_label)

    # rename label, >0 => 1, =0 or empty => -1
    streaming_rename_label = Task.create('HadoopStreaming', name='streaming_rename_label')
    streaming_rename_label.upstream = [join_show_app_site_label]
    output_streaming_rename_label.schema = join_show_app_site_label.outputs[0].schema
    streaming_rename_label.outputs = [output_streaming_rename_label]
    streaming_rename_label.set_conf('conf', '%s/streaming_rename_label.yaml'%conf_root)
    streaming_rename_label.solid()
    task_list.append(streaming_rename_label)

    # split training & eval, < 10.29 train, >=10.29 eval
    streaming_split_train = Task.create('HadoopStreaming', name='streaming_split_train')
    streaming_split_train.upstream = [streaming_rename_label]
    output_streaming_split_train.schema = streaming_split_train.inputs[0].schema
    streaming_split_train.outputs = [output_streaming_split_train]
    streaming_split_train.set_conf('conf', '%s/streaming_split_train.yaml'%conf_root)
    streaming_split_train.solid()
    task_list.append(streaming_split_train)

    streaming_split_eval = Task.create('HadoopStreaming', name='streaming_split_eval')
    streaming_split_eval.upstream = [streaming_rename_label]
    output_streaming_split_eval.schema = streaming_split_eval.inputs[0].schema
    streaming_split_eval.outputs = [output_streaming_split_eval]
    streaming_split_eval.set_conf('conf', '%s/streaming_split_eval.yaml'%conf_root)
    streaming_split_eval.solid()
    task_list.append(streaming_split_eval)

    # feature extract
    fe_train = Task.create('FeatureExtraction', name='feture_extract_train')
    fe_train.set_conf('conf', '%s/feature_list.config'%conf_root)
    fe_train.upstream = [streaming_split_train]
    fe_train.outputs = [output_feature_extract_train]
    fe_train.solid()
    task_list.append(fe_train)

    fe_eval = Task.create('FeatureExtraction', name='feture_extract_eval')
    fe_eval.set_conf('conf', '%s/feature_list.config'%conf_root)
    fe_eval.upstream = [streaming_split_eval]
    fe_eval.outputs = [output_feature_extract_eval]
    fe_eval.solid()
    task_list.append(fe_eval)

    # shuffle training instance
    streaming_shuffle_train = Task.create('HadoopStreaming', name='streaming_shuffle_train')
    streaming_shuffle_train.upstream = [fe_train]
    streaming_shuffle_train.outputs = [output_shuffle_train]
    streaming_shuffle_train.set_conf('conf', '%s/streaming_shuffle.yaml'%conf_root)
    streaming_shuffle_train.solid()
    task_list.append(streaming_shuffle_train)

    # model train
    model_train = Task.create('PicoTraining', name='model_train')
    model_train.outputs = [output_train]
    model_train.set_conf('feature_creation_ratio', 0.9)
    model_train.upstream = [streaming_shuffle_train]
    model_train.solid()
    task_list.append(model_train)

    # model eval
    model_eval = Task.create('PicoTesting', name='model_evaluate')
    model_eval.outputs = [output_eval]
    model_eval.upstream = [fe_eval, model_train]
    model_eval.solid()
    task_list.append(model_eval)

    plan = Plan.create('ctr_demo')
    plan.pipeline(task_list)
    plan.run()
    print 'Plan ID=%s is running' % plan.id
    plan.wait()
    print 'Plan ID=%s finished, status=%s' % (plan.id, plan.status)


def resume_plan(plan_id):
    plan = Plan.query(plan_id)
    # reload config
    some_task = plan.tasks[xxx]
    some_task.set_conf(xxx, xxx)
    some_task.solid()
    plan.resume()
    print 'Plan ID=%s is resuming' % plan.id
    plan.wait()
    print 'Plan ID=%s finished, status=%s' % (plan.id, plan.status)


def run_single_task():
    i_d = Data.query('output_data')
    o_d = Data.create('output_model')

    model_train = Task.create('PicoTraining')
    model_train.inputs = [i_d]
    model_train.outputs = [o_d]

    # configure
    model_train.set_conf('feature_creation_ratio', 1.0)
    model_train.run()


def delete_data(name):
    # delete_really: False: mark as deleted in DB, cannot be used, but cannot re-create it
    #                True: delete entry in DB, can be re-created
    d = Data.delete(name, delete_really=False) 
    print 'Data %s has been marked as delete, uri=%s' % (name, d.uri)


if __name__ == '__main__':
    run_plan()
    #resume_plan(plan_id)

