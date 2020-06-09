# -*- coding: utf-8 -*-

import redis
import logging
import requests
import numpy as np
import os
from datetime import datetime, timedelta


def get_bct_host():
    return os.getenv('BCT_HOST', 'localhost')


def get_bct_port():
    return os.getenv('BCT_PORT', '16016')


def get_redis_conn(ip):
    redis_ip = os.getenv('REDIS_HOST', ip)
    return redis.Redis(host=redis_ip, port=6379, db=0)


def login(login_ip, login_body):
    login_url = 'http://' + login_ip + ':' + get_bct_port() + '/auth-service/users/login'
    login_res = requests.post(login_url, json=login_body)
    token = login_res.json()['result']['token']
    headers = {
        'Authorization': 'Bearer ' + token
    }
    return headers


def call_request(ip, service, method, params, headers):
    url = 'http://' + ip + ':' + get_bct_port() + '/' + service + '/api/rpc'
    body = {
        'method': method,
        'params': params
    }
    try:
        res = requests.post(url, json=body, headers=headers)
        json = res.json()
        if 'error' in json:
            logging.info("failed execute " + method + " to: " + ip + ", error: " + json['error']['message'])
            return 'error'
        else:
            logging.info("success execute " + method + ", callRequest: " + str(len(params)) + " to " + ip)
            return json['result']
    except Exception as e:
        logging.info("failed execute " + method + " to:" + ip + " Exception: " + str(e))
        raise e


def call(method, params, service, host, headers):
    url = 'http://' + host + ':' + get_bct_port() + '/' + ('' if service is None else (service + '/')) + 'api/rpc'
    body = {
        "method": method,
        "params": params
    }
    res = requests.post(url, json=body, headers=headers)
    json = res.json()
    if 'error' in json:
        raise RuntimeError('error calling method {method}: {msg}'.format(method=method, msg=json['error']))
        # print('error calling method {method}: {msg}'.format(method=method, msg=json['error']['message']))
    else:
        return json['result']
