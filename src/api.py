from flask import Blueprint, request, current_app
from flask_cors import cross_origin
import json
import os
import pytz
import requests
import uuid
import sys
import traceback
from typing import List
from datetime import datetime, timedelta
from functools import wraps
import jwt
from jwt import \
    DecodeError, MissingRequiredClaimError, \
    InvalidIssuedAtError, ImmatureSignatureError, \
    ExpiredSignatureError

# wrappers
from .framework.src.task_queue.wrapper.amqp import AMQP
from .framework.src.task_queue.wrapper.redis_client import RedisClient
from .framework.src.task_queue.wrapper.mongodb_client import MongoDBClient
# settings
from .framework.src.task_queue.common.settings import \
    workflow_status_redis_key, task_log_status_redis_key, \
    task_status_redis_key, \
    heartbeat_redis_key, heartbeat_sleep_time
# mongodb models
from .framework.src.task_queue.models.mongo.workflow_log import WorkflowLog
from .framework.src.task_queue.models.mongo.task import Task
# redis models
from .framework.src.task_queue.models.redis.workflow_status import WorkflowStatus
from .framework.src.task_queue.models.redis.task_log_status import TaskLogStatus
from .framework.src.task_queue.models.redis.heartbeat import Heartbeat
from .framework.src.task_queue.models.redis.task_status import TaskStatus
from .framework.src.task_queue.models.redis.task_control_message import TaskControlMessage

# login api
from .login_api.login_api import roles_required


app = Blueprint(
    'api',
    __name__)

redis_host = os.getenv('REDIS_HOST', '192.168.6.1')
rabbitmq_host = os.getenv('RABBITMQ_HOST', '192.168.6.1')
rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')
authentication_api_host = \
    os.getenv('AUTHENTICATION_API_HOST', 'http://192.168.6.1:8083')
mongodb_uri = os.getenv(
    'MONGODB_CONNECTION_URI',
    'mongodb://root:example@192.168.6.1/orchestrator_db?authSource=admin'
)

redis_client: RedisClient = RedisClient(redis_host)
amqp_client = AMQP(
    host=rabbitmq_host,
    queue_name='task_queue',
    username=rabbitmq_user,
    password=rabbitmq_password,
    amqp_type='publisher'
)
mongodb_client = MongoDBClient(mongodb_uri)


@app.route('/task_log/<string:task_id>')
@cross_origin()
@roles_required(roles=['USER'])
def task_log(task_id: str):
    return json.dumps(
        [
            task
            for task in mongodb_client.db().logs.find(
                {'task_id': task_id},
                {'log_line': 1, '_id': 0}
            )
        ]
    )


@app.route('/workflow_status/<string:workflow_id>')
@cross_origin()
@roles_required(roles=['USER'])
def workflow_status(workflow_id: str):
    workflow_status_len = redis_client.llen(workflow_status_redis_key)
    for i in range(0, workflow_status_len):
        list_obj: WorkflowStatus = redis_client.lindex_obj(
            workflow_status_redis_key, i, WorkflowStatus)
        if (
            list_obj is not None and
            list_obj.workflow_id == workflow_id
        ):
            return list_obj.to_json(), 200
    return WorkflowStatus(workflow_id=workflow_id, status=0).to_json(), 404


def task_status(task_id: str):
    task_status_len = redis_client.llen(task_log_status_redis_key)
    for i in range(0, task_status_len):
        list_obj: TaskLogStatus = redis_client.lindex_obj(
            task_log_status_redis_key, i, TaskLogStatus)
        if (
            list_obj is not None and
            list_obj.task_id == task_id
        ):
            return 1
    return 0


@app.route('/workflow_details/<string:workflow_id>')
@cross_origin()
@roles_required(roles=['USER'])
def workflow_details(workflow_id: str):
    return Task.schema().dumps(workflow_tasks(workflow_id))


def workflow_tasks(workflow_id: str):
    task_ids: List[Task] = []
    workflow_task_entries = mongodb_client.db().tasks.find(
        {'workflow_id': workflow_id}
    )
    print(workflow_task_entries)
    for task in workflow_task_entries:
        task_ids.append(task['task'])
    return task_ids


@app.route('/workflow_log/<string:workflow_id>')
@cross_origin()
@roles_required(roles=['USER'])
def workflow_log(workflow_id: str):
    workflow_log = []
    task_ids: List[Task] = workflow_tasks(workflow_id)
    for task in task_ids:
        task_log: List[str] = []
        task_log_entries = mongodb_client.db().logs.find(
            {'task_id': task['task_id']}
        )
        for log_line in task_log_entries:
            task_log.append(log_line['log_line'])
        workflow_log.append(
            WorkflowLog(
                 log_lines=task_log,
                 task_id=task['task_id'],
                 workflow_id=workflow_id
             )
        )
    return WorkflowLog.schema().dumps(workflow_log, many=True)


@app.route('/task_details/<string:task_id>')
@cross_origin()
@roles_required(roles=['USER'])
def task_details(task_id: str):
    task = mongodb_client.db().tasks.find_one({'task.task_id': task_id})
    filtered_task = {
        task_key: task[task_key] for task_key in task if task_key != '_id'
    }
    return \
        (json.dumps(filtered_task), 200) if filtered_task is not None \
        else (json.dumps(''), 404)


@app.route('/new_task/<string:node_name>/<string:task>', methods=['POST'])
@cross_origin()
@roles_required(roles=['USER'])
def new_task(task: str, node_name: str):
    json_body = request.json
    if 'tags' in json_body:
        tags = json_body['tags']
    else:
        tags = None
    if 'arguments' in json_body:
        arguments = json_body['arguments']
    else:
        arguments = None
    if node_name == 'default':
        amqp_client.send(
            Task(
                name=task,
                arguments=arguments,
                tags=tags
            ).to_json())
    else:
        new_task: Task = Task(
            name=task,
            arguments=arguments,
            node_names=[node_name],
            tags=tags
        )
        print('new_task: ' + new_task.to_json())
        amqp_client.send(new_task.to_json())
    return json.dumps('OK'), 200


@app.route('/available_tasks')
@cross_origin()
@roles_required(roles=['USER'])
def tasks():
    return json.dumps([
        {
            task_key: task[task_key] for task_key in task if task_key != '_id'
        }
        for task in mongodb_client.db().registered_tasks.find({})
    ]), 200


@app.route('/workflow_history_count/<string:node_name>')
@cross_origin()
@roles_required(roles=['USER'])
def workflow_history_count(node_name: str):
    return json.dumps(
        mongodb_client.db().workflows.count()
        if node_name == 'default'
        else mongodb_client.db().workflows.find(
            {
                'node_name': node_name
            }
        ).count()
    ), 200


@app.route('/workflow_history/<string:node_name>/<int:begin>/<int:end>')
@cross_origin()
@roles_required(roles=['USER'])
def workflow_history(node_name: str, begin: int, end: int):
    return json.dumps(
        [
            {
                task_key: workflow[task_key]
                for task_key in workflow if task_key != '_id'
            }
            for workflow in
            mongodb_client.db().workflows.find({}).skip(begin).limit(end)
        ]
        if node_name == 'default'
        else [
            {
                task_key: workflow[task_key]
                for task_key in workflow if task_key != '_id'
            }
            for workflow in
            mongodb_client.db().workflows.find(
                {
                    'node_name': node_name
                }
            ).skip(begin).limit(end)
        ]
    )


@app.route('/stop_workflow/<string:workflow_id>', methods=['POST'])
@cross_origin()
@roles_required(roles=['USER'])
def stop_workflow(workflow_id: str):
    redis_client.publish(
        'task_control_channel',
        TaskControlMessage(
            workflow_id=workflow_id,
            command='stop'
        ).to_json())
    return json.dumps('OK'), 200


@app.route('/abort_workflow/<string:workflow_id>', methods=['POST'])
@cross_origin()
@roles_required(roles=['USER'])
def abort_workflow(workflow_id: str):
    redis_client.publish(
        'task_control_channel',
        TaskControlMessage(
            workflow_id=workflow_id,
            command='abort'
        ).to_json())
    return json.dumps('OK'), 200


@app.route('/task_metrics/<string:begin>/<string:end>')
@cross_origin()
@roles_required(roles=['USER'])
def task_metrics(begin: str, end: str):
    task_status_length = redis_client.llen(task_status_redis_key)
    metrics: List[str] = []
    for i in range(0, task_status_length):
        list_obj: TaskStatus = redis_client.lindex_obj(
            task_status_redis_key, i, TaskStatus)
        if (
            list_obj is not None and
            (datetime.now(tz=pytz.UTC) - list_obj.finished_date).days <= 10
        ):
            metrics.append(
                {
                    'date': list_obj.finished_date.replace(
                        second=0,
                        microsecond=0
                    )
                }
            )
    metrics = {
        k['date'].isoformat(): metrics.count(k)
        for k in metrics if k.get('date')
    }
    metrics = [
        {'count': metrics[k], 'date': k}
        for k in metrics
    ]
    return json.dumps(metrics), 200


@app.route('/all_nodes')
@cross_origin()
@roles_required(roles=['USER'])
def node_names():
    return json.dumps(
        [
            node['node_name']
            for node in
            mongodb_client.db().registered_tasks.find(
                {},
                {'node_name': 1, '_id': 0}
            )
        ]
    )


@app.route('/active_nodes')
@cross_origin()
@roles_required(roles=['USER'])
def nodes():
    node_list: List[str] = []
    node_name_list = mongodb_client.db().registered_tasks.find(
        {},
        {'node_name': 1, '_id': 0}
    )
    for node_name in node_name_list:
        node_status_bytes = redis_client.get(
            heartbeat_redis_key + '_' + node_name['node_name'])
        if node_status_bytes is not None:
            node_status_string = node_status_bytes.decode('utf-8')
            if len(node_status_string) > 0:
                heartbeat_status: Heartbeat = \
                    Heartbeat.from_json(node_status_string)
                last_time_seen = heartbeat_status.last_time_seen
                now = datetime.utcnow().replace(tzinfo=pytz.utc)
                diff = now - last_time_seen
                if diff.total_seconds() <= heartbeat_sleep_time * 2:
                    node_list.append(node_name['node_name'])
    return json.dumps(node_list)
