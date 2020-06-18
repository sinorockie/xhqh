import requests

from python.imports.init_params import *


# class TerminalServerConfig:
#     host = 'terminal-prod2.tongyu-quant.com'
#     port = 16016
#     username = 'admin'
#     password = '123456Aa'
#     client_id = 'terminal'
#     grant_type = 'password'

class TerminalServerConfig:
    host = terminal_host
    port = terminal_port
    username = terminal_username
    password = terminal_password
    client_id = terminal_client_id
    grant_type = terminal_grant_type


def login_terminal():
    def login(login_ip, login_port, login_body):
        login_url = 'https://' + login_ip + '/terminal-auth-service/oauth/token'
        # login_url = 'https://terminal-prod2.tongyu-quant.com/terminal-auth-service/oauth/token'
        login_res = requests.post(login_url, data=login_body)
        token = login_res.json()['access_token']
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token
        }
        return headers

    terminal_login_body = {
        'username': TerminalServerConfig.username,
        'password': TerminalServerConfig.password,
        'client_id': TerminalServerConfig.client_id,
        'grant_type': TerminalServerConfig.grant_type
    }
    return login(TerminalServerConfig.host, TerminalServerConfig.port, terminal_login_body)


def call_terminal_request(service, method, params, headers):
    def call_request(ip, port, service, method, params, headers):
        if service is not None:
            url = 'https://' + ip + '/' + service + '/api/rpc'
        else:
            url = 'https://' + ip + '/api/rpc'
        # url = 'https://terminal-prod2.tongyu-quant.com/{}/api/rpc'.format(service)
        body = {
            'jsonrpc': '2.0',
            'method': method,
            'id': 1,
            'params': params
        }
        try:
            res = requests.post(url, json=body, headers=headers)
            json = res.json()
            if 'error' in json:
                print("failed execute " + method + " to:" + ip + ",error:" + json['error']['message'])
                return 'error'
            else:
                print("success execute " + method + ",callRequest:" + str(len(params)) + " to " + ip)
                return json
        except Exception as e:
            print("failed execute " + method + " to:" + ip + "Exception:" + str(e))
            raise e

    return call_request(TerminalServerConfig.host, TerminalServerConfig.port, service, method, params, headers)[
        'result']


def download(body, out_file_path, path, headers):
    headers['Accept'] = '*/*'
    url = 'https://' + TerminalServerConfig.host + '/terminal-service/' + path
    try:
        res = requests.post(url, json=body, headers=headers)
        with open(out_file_path, "wb") as f:
            f.write(res.content)
    except Exception as e:
        print("导出终端文件失败 Exception:" + str(e))
        raise e
