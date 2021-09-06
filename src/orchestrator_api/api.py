from fastapi import APIRouter, Depends, Request
from fastapi.param_functions import Body, Query
from pydantic.main import BaseModel
import pytz
import bson
from typing import List, Optional
from datetime import datetime
import re
import time
# import json

# wrappers
from framework.src.chain_factory.task_queue.wrapper.amqp import AMQP

# settings
from framework.src.chain_factory.task_queue.common.settings import (
    heartbeat_redis_key,
    heartbeat_sleep_time,
    task_control_channel_redis_key,
)

# mongodb models
from framework.src.chain_factory.task_queue.models.mongo.task import Task

# redis models
from framework.src.chain_factory.task_queue.models.redis.heartbeat import \
    Heartbeat
from framework.src.chain_factory.task_queue.models.redis.task_control_message \
    import (
        TaskControlMessage,
    )

from login_api.login_api2 import (
    RolesRequiredChecker,
)


def default_namespace(namespace):
    return namespace == "default" or namespace == "all"


def has_pagination(page, page_size):
    return page is not None and page_size is not None


def get_page_size(page_size):
    return page_size if page_size > 0 else 1


def skip_stage(page, page_size):
    stage = {}
    if has_pagination(page, page_size):
        stage["$skip"] = page * get_page_size(page_size)
    return stage


def limit_stage(page, page_size):
    stage = {}
    if has_pagination(page, page_size):
        stage["$limit"] = get_page_size(page_size)
    return stage


def sort_stage(sort_by, sort_order):
    stage = {}
    sortable_fields = [
        'created_date',
        'entry_task.name',
        'workflow.workflow_id',
        'workflow.namespace',
        'workflow.tags'
    ]
    if sort_by in sortable_fields and sort_order in ["asc", "desc"]:
        stage['$sort'] = {
            sort_by: 1 if sort_order == 'asc' else -1
        }
    return stage


def lookup(from_: str, local_field: str, foreign_field: str, as_: str):
    return {
        '$lookup': {
            'from': from_,
            'localField': local_field,
            'foreignField': foreign_field,
            'as': as_
        }
    }


def lookup_workflow_status(local_field: str, as_: str):
    return lookup('workflow_status', local_field, 'workflow_id', as_)


def lookup_logs(local_field: str, as_: str):
    return lookup('logs', local_field, 'task_id', as_)


def match(fields):
    return {
        "$match": fields
    }


def unwind(field):
    return {
        "$unwind": field
    }


def project(fields):
    return {
        "$project": fields
    }


def get_amqp_client(namespace: str, request: Request):
    return AMQP(
        host=request.state.rabbitmq_host,
        queue_name=namespace + "_" + "task_queue",
        username=request.state.rabbitmq_user,
        password=request.state.rabbitmq_password,
        amqp_type="publisher",
        virtual_host=namespace,
    )


app = APIRouter()
user_role = Depends(RolesRequiredChecker(roles=["USER"]))


class NewTaskRequest(BaseModel):
    arguments: dict = {}
    tags: List[str] = []


@app.post("/new_task", dependencies=[user_role])
def new_task(
    request: Request,
    namespace: str,
    task: str,
    node_name: str,
    json_body: Optional[NewTaskRequest] = Body(...),
):
    if node_name == "default":
        amqp_client = get_amqp_client(namespace, request)
        for i in range(0, 1):
            amqp_client.send(
                Task(
                    name=task,
                    arguments=json_body.arguments,
                    tags=json_body.tags
                ).to_json()
            )
        amqp_client.close()
    else:
        new_task: Task = Task(
            name=task,
            arguments=json_body.arguments,
            node_names=[node_name],
            tags=json_body.tags,
        )
        amqp_client = get_amqp_client(namespace, request)
        for i in range(0, 1):
            amqp_client.send(new_task.to_json())
        amqp_client.close()
    return {"status": "OK"}


async def node_active(node_name: str, namespace: str, request: Request):
    redis_key = heartbeat_redis_key + "_" + namespace + "_" + node_name
    node_status_bytes = request.state.redis_client.get(redis_key)
    if node_status_bytes is not None:
        node_status_string = node_status_bytes.decode("utf-8")
        if len(node_status_string) > 0:
            heartbeat_status: Heartbeat = Heartbeat.from_json(
                node_status_string)
            last_time_seen = heartbeat_status.last_time_seen
            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            diff = now - last_time_seen
            if diff.total_seconds() <= (heartbeat_sleep_time * 2):
                return True
    return False


async def nodes(namespace: str, request: Request):
    node_list: List[str] = []

    def match_namespace():
        query = {}
        if not default_namespace(namespace):
            query["namespace"] = namespace
        return query

    node_name_list = request.state.database.registered_tasks.find(
        match_namespace(), {"node_name": 1, "_id": 0, "namespace": 1}
    )
    async for node_name in node_name_list:
        if (
            await node_active(
                node_name["node_name"],
                node_name["namespace"],
                request
            )
        ):
            node_list.append(node_name)
    return node_list


async def tasks(
    namespace: str,
    request: Request,
    search: str,
    page: int = None,
    page_size: int = None,
    nodes=[],
):
    unwind_stage = unwind("$tasks")

    def match_stage():
        stage = match({})
        if search:
            rgx = bson.regex.Regex("^{}".format(search))
            stage["$match"] = {"tasks.name": {"$regex": rgx}}
        if not default_namespace(namespace):
            stage["$match"]["namespace"] = namespace
        if nodes or nodes is None:
            stage["$match"]["node_name"] = {
                "$in": [node["node_name"] for node in (nodes if nodes else [])]
            }
            stage["$match"]["namespace"] = {
                "$in": [node["namespace"] for node in (nodes if nodes else [])]
            }
        return stage

    aggregate_query = [
        stage
        for stage in [
            match_stage(),
            unwind_stage,
            match_stage(),
            project({"_id": 0}),
            {
                "$facet": {
                    "node_tasks": [
                        stage2
                        for stage2 in [
                            skip_stage(page, page_size),
                            limit_stage(page, page_size)
                        ]
                        if stage2 != {}
                    ],
                    "total_count": [{"$count": "count"}],
                }
            },
            project({
                "node_tasks": 1,
                "total_count": {"$first": "$total_count.count"},
            }),
        ]
        if stage != {}
    ]
    registered_tasks_result = \
        request.state.database.registered_tasks.aggregate(
            aggregate_query
        )
    registered_tasks = [tasks async for tasks in registered_tasks_result]
    return (
        registered_tasks[0]
        if len(registered_tasks) > 0
        else {"node_tasks": [], "total_count": 0}
    )


@app.get("/active_tasks", dependencies=[user_role])
async def active_tasks(
    namespace: str,
    request: Request,
    search: Optional[str] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
):
    # time.sleep(0.5)
    active_nodes = await nodes(namespace, request)
    tasks_result = await tasks(
        namespace,
        request,
        search,
        page,
        page_size,
        active_nodes if active_nodes else None,
    )
    return tasks_result


@app.get('/workflows', dependencies=[user_role])
async def workflows(
    namespace: str,
    request: Request,
    search: Optional[str] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    sort_by: Optional[str] = None,
    sort_order: Optional[str] = None,
):
    # time.sleep(5)

    def search_stage():
        stages = []
        stage = match({
            '$and': [
                {
                    "workflow.namespace": {
                        '$exists': 'true',
                        '$nin': ["", 'null']
                    },
                }
            ]
        })
        if search:
            search_splitted = search.split(' ')
            patterns = []
            keys = []

            def get_regex(pattern):
                regex = re.compile(pattern)
                rgx = bson.regex.Regex.from_native(regex)
                rgx.flags ^= re.UNICODE
                return rgx

            operators = {
                'name': {'type': 'str', 'key': 'tasks.name'},
                'tags': {'type': 'list', 'key': 'tasks.tags'},
                'namespace': {'type': 'str', 'key': 'workflow.namespace'},
                'date': {'type': 'str', 'key': 'created_date'},
                'arguments': {'type': 'dict', 'key': 'tasks.arguments'},
                'logs': {'type': 'logs', 'key': 'logs.log_line'}
            }

            def get_operator(operator, value):
                operator_obj = operators[operator]
                if (
                    operator_obj['type'] == 'str' or
                    operator_obj['type'] == 'list'
                ):
                    return {
                        operator_obj['key']: {
                            '$regex': get_regex(value)
                        }
                    }, None
                elif operator_obj['type'] == 'dict':
                    splitted_map = value.split(':')
                    if len(splitted_map) >= 2:
                        joined_map = ''.join(splitted_map[1:])
                        splitted_map[1] = joined_map
                    if len(splitted_map) < 2:
                        return None, None
                    project_stage = {
                        '$addFields': {
                            '{}_string'.format(splitted_map[0]): {
                                "$map": {
                                    "input": "${}.{}".format(
                                        operator_obj['key'],
                                        splitted_map[0]
                                    ),
                                    "as": "row",
                                    "in": {
                                        "value": {"$toString": '$$row'}
                                    }
                                }
                            },
                        }
                    }
                    stages.append(project_stage)
                    return {
                        '{}_string.value'.format(splitted_map[0]): {
                            '$regex': get_regex(splitted_map[1])
                        }
                    }, '{}_string'.format(splitted_map[0])
                elif operator_obj['type'] == 'logs':
                    stages.append(
                        lookup_logs('tasks.task_id', 'logs')
                    )
                    return {
                        'logs.log_line': {
                            '$regex': get_regex(value)
                        }
                    }, 'logs'
                return None, None
            for search_pattern in search_splitted:
                if ':' in search_pattern:
                    tokens = search_pattern.split(':')
                    operator, value = tokens[0], ':'.join(tokens[1:])
                    if operator in operators:
                        pattern_temp, key = get_operator(operator, value)
                        if key:
                            keys.append(key)
                        if pattern_temp:
                            patterns.append(pattern_temp)
                else:
                    pattern_temp, key = get_operator('name', search_pattern)
                    if pattern_temp:
                        patterns.append(pattern_temp)
            if patterns:
                stage['$match']['$and'].append({
                    '$and': patterns
                })
        if namespace and not default_namespace(namespace):
            stage['$match']['$and'].append({
                'workflow.namespace': namespace
            })
        stages.append(stage)
        if len(stages) >= 2:
            stages.append(project({key: 0 for key in keys}))
        return stages

    pipeline = [
        stage for stage in [
            {
                '$group': {
                    '_id': {'workflow_id': '$workflow_id'},
                    'workflow': {
                        '$addToSet': {
                            'workflow_id': '$workflow_id',
                            'namespace': '$namespace',
                            'tags': '$tags',
                        },
                    },
                    'created_dates': {
                        '$push': '$created_date'
                    }
                }
            },
            lookup('tasks', '_id.workflow_id', 'workflow_id', 'tasks'),
            project({
                '_id': 0,
                'tasks._id': 0,
                'tasks.task.workflow_id': 0,
                'tasks.workflow_id': 0,
                'tasks.node_name': 0
            }),
            project({
                'tasks': '$tasks.task',
                "workflow": {
                    "$first": "$workflow"
                },
                'created_date': {
                    '$first': '$created_dates'
                }
            }),
            *search_stage(),
            project({
                'entry_task': {'$first': '$tasks'},
                'workflow': 1,
                'created_date': 1,
            }),
            lookup_workflow_status('workflow.workflow_id', 'status'),
            project({
                'entry_task': 1,
                'workflow': 1,
                'created_date': 1,
                'status': {
                    '$ifNull': [
                        {'$first': '$status.status'},
                        'Running'
                    ]
                },
            }),
            project({'status._id': 0}),
            {
                "$facet": {
                    "workflows": [
                        stage2
                        for stage2 in [
                            sort_stage(sort_by, sort_order),
                            skip_stage(page, page_size),
                            limit_stage(page, page_size)
                        ]
                        if stage2 != {}
                    ],
                    "total_count": [{"$count": "count"}],
                }
            },
            project({
                "workflows": 1,
                "total_count": {
                    '$ifNull': [{"$first": "$total_count.count"}, 0]
                },
                'count': {'$size': '$workflows'}
            })
        ] if stage != {}
    ]

    # print(json.dumps(
    #     pipeline,
    #     indent=4,
    #     default=lambda o: '<not serializable>'
    # ))

    workflow_tasks = request.state.database.workflows.aggregate(pipeline)
    return (await workflow_tasks.to_list(1))[0]


@app.get('/namespaces', dependencies=[user_role])
async def namespaces(request: Request):
    namespaces_result = [
        {
            namespace_key: namespace[namespace_key]
            for namespace_key in namespace if namespace_key != '_id'
        }
        async for namespace in request.state.database.namespaces.find({})
    ]
    return namespaces_result


@app.get('/task_logs', dependencies=[user_role])
async def task_log(
    task_id: str,
    request: Request,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
):
    log_result = request.state.database.logs.aggregate([
        match({'task_id': task_id}),
        project({
            'log_line': 1,
            '_id': 0,
        }),
        {
            "$facet": {
                "log_lines": [
                    stage2
                    for stage2 in [
                        skip_stage(page, page_size),
                        limit_stage(page, page_size)
                    ]
                    if stage2 != {}
                ],
                "total_count": [{"$count": "count"}],
            }
        },
        project({
            'log_lines': '$log_lines.log_line',
            'total_count': {
                '$first': '$total_count.count'
            }
        })
    ])
    return (await log_result.to_list(1))[0]


@app.get('/workflow_logs', dependencies=[user_role])
async def workflow_logs(
    workflow_id: str,
    request: Request,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
):
    log_result = request.state.database.tasks.aggregate([
        lookup_logs('task.task_id', 'logs'),
        project({
            '_id': 0,
            'logs._id': 0,
            'logs.task_id': 0,
            'task.name': 0,
            'task.arguments': 0
        }),
        match({'workflow_id': workflow_id}),
        project({
            'task_id': '$task.task_id',
            'logs': '$logs.log_line'
        }),
        {
            "$facet": {
                "task_logs": [
                    stage2
                    for stage2 in [
                        skip_stage(page, page_size),
                        limit_stage(page, page_size)
                    ]
                    if stage2 != {}
                ],
                "total_count": [{"$count": "count"}],
            }
        },
        project({
            'task_logs': 1,
            'total_count': {
                '$first': '$total_count.count'
            }
        }),
    ])
    return (await log_result.to_list(1))[0]


@app.get('/workflow_tasks', dependencies=[user_role])
async def workflow_tasks(
    workflow_id: str,
    request: Request,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
):
    log_result = request.state.database.tasks.aggregate([
        project({'_id': 0}),
        match({'workflow_id': workflow_id}),
        {
            "$facet": {
                "tasks": [
                    stage2
                    for stage2 in [
                        skip_stage(page, page_size),
                        limit_stage(page, page_size)
                    ]
                    if stage2 != {}
                ],
                "total_count": [{"$count": "count"}],
            }
        },
        project({
            'tasks': '$tasks.task',
            'total_count': {
                '$first': '$total_count.count'
            }
        })
    ])
    return (await log_result.to_list(1))[0]


@app.get('/workflow_status', dependencies=[user_role])
async def workflow_status(
    request: Request,
    workflow_id: List[str] = Query([]),
):
    def match_stage():
        stage = match({})
        if isinstance(workflow_id, str):
            stage["$match"]['workflow_id'] = workflow_id
        elif isinstance(workflow_id, list):
            stage["$match"]['workflow_id'] = {
                '$in': workflow_id
            }
        return stage
    log_result = request.state.database.workflows.aggregate([
        match_stage(),
        lookup_workflow_status('workflow_id', 'workflow_status'),
        project({
            'status': {
                '$ifNull': [
                    {
                        '$first': "$workflow_status.status"
                    },
                    'Running'
                ]
            },
            'workflow_id': 1,
            'workflow_status': 1,
            '_id': 0
        }),
        lookup('tasks', 'workflow_id', 'workflow_id', 'tasks'),
        project({
            'status': '$status',
            'workflow_id': 1,
            'tasks.task.task_id': 1,
        }),
        lookup('task_status', 'tasks.task.task_id', 'task_id', 'task_status1'),
        {
            "$addFields": {
                "tasks": {
                    "$map": {
                        "input": "$tasks",
                        "as": "row",
                        "in": {
                            '$mergeObjects': [
                                "$$row",
                                {
                                    '$first': {
                                        '$filter': {
                                            'input': "$task_status1",
                                            'cond': {
                                                '$eq': [
                                                    "$$this.task_id",
                                                    "$$row.task.task_id"
                                                ]
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        },
        project({
            'status': 1,
            'workflow_id': 1,
            'tasks': {
                "$map": {
                    "input": "$tasks",
                    "as": "row",
                    "in": {
                        "task_id": '$$row.task.task_id',
                        "status": {'$ifNull': ["$$row.status", 'Running']}
                    }
                }
            }
        })
    ])
    return (await log_result.to_list(None))


@app.post(
    '/stop_workflow',
    dependencies=[
        Depends(RolesRequiredChecker(roles=["WORKFLOW_CONTROLLER"]))
    ]
)
async def stop_workflow(namespace: str, workflow_id: str, request: Request):
    request.state.redis_client.publish(
        namespace + '_' + task_control_channel_redis_key,
        TaskControlMessage(
            workflow_id=workflow_id,
            command='stop'
        ).to_json())
    workflow_status = request.state.database.workflow_status
    if not (
        await workflow_status.find_one({
            'workflow_id': workflow_id,
            'namespace': namespace
        })
    ):
        await workflow_status.insert_one({
            'workflow_id': workflow_id,
            'namespace': namespace,
            'status': 'Stopped',
            'created_date': datetime.now(pytz.UTC)
        })
    return 'OK'


@app.post(
    '/abort_workflow',
    dependencies=[
        Depends(RolesRequiredChecker(roles=["WORKFLOW_CONTROLLER"]))
    ]
)
async def abort_workflow(namespace: str, workflow_id: str, request: Request):
    request.state.redis_client.publish(
        namespace + '_' + task_control_channel_redis_key,
        TaskControlMessage(
            workflow_id=workflow_id,
            command='abort'
        ).to_json())
    workflow_status = request.state.database.workflow_status
    if not (
        await workflow_status.find_one(
            {'workflow_id': workflow_id, 'namespace': namespace}
        )
    ):
        await workflow_status.insert_one({
            'workflow_id': workflow_id,
            'namespace': namespace,
            'status': 'Stopped'
        })
    return 'OK'


@app.post(
    '/stop_node',
    dependencies=[
        Depends(RolesRequiredChecker(roles=["NODE_ADMIN"]))
    ]
)
def stop_node(namespace: str, node_name: str, request: Request):
    request.state.redis_client.publish(
        namespace + '_' + 'node_control_channel',
        TaskControlMessage(
            workflow_id=node_name,
            command='stop'
        ).to_json())
    return 'OK'


@app.get('/node_metrics', dependencies=[user_role])
async def node_metrics(namespace: str, request: Request):
    registered_tasks = request.state.database.registered_tasks
    if default_namespace(namespace):
        registered_tasks_result = await registered_tasks.find({}).to_list(None)
    else:
        registered_tasks_result = await registered_tasks.find(
            {'namespace': namespace}).to_list(None)
    nodes = {}
    for task in registered_tasks_result:
        node_name = task['node_name']
        node_namespace = task['namespace']
        if node_name:
            node_status = await node_active(node_name, node_namespace, request)
            nodes[node_name] = node_status
    return nodes


@app.get('/workflow_metrics', dependencies=[])
async def workflow_metrics(namespace: str, request: Request):
    def match_stage():
        stage = match({})
        if not default_namespace(namespace):
            stage['$match']['namespace'] = namespace
        return stage
    pipeline = [
        match_stage(),
        lookup_workflow_status('workflow_id', 'workflow_status'),
        project({
            'status': {
                '$ifNull': [
                    {
                        '$first': "$workflow_status.status"
                    },
                    'Running'
                ]
            },
            'workflow_id': 1,
            'created_date': {
                '$ifNull': [
                    {
                        '$toString': {
                            '$last': "$workflow_status.created_date"
                        },
                    },
                    {
                        '$toString': "$created_date"
                    }
                ]
            },
            'namespace': 1,
        }),
        project({
            '_id': 0,
            'workflow_status._id': 0
        })
    ]
    print(pipeline)
    workflow_status_result2 = request.state.database.workflows.aggregate(
        pipeline)
    return await workflow_status_result2.to_list(None)
