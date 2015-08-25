#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import requests
from datacanvas.utils import url_path_join


class EhcClient(object):
    def __init__(self, ehc_token, ehc_id, ehc_api_root="https://ehc.datacanvas.io:4430"):
        self.ehc_token = ehc_token
        self.ehc_id = ehc_id
        self.api_url = ehc_api_root

    def _post_command(self, cmd_path, cmd_type, **cmd_args):
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-TOKEN": self.ehc_token
        }
        cmd_body = {
            "ehc_setting": {
                "id": self.ehc_id
            },
            "command_setting": {
                "type": cmd_type,
                "conf": cmd_args
            }
        }

        return requests.post(url_path_join(self.api_url, cmd_path),
                             headers=headers,
                             data=json.dumps(cmd_body))

    def _get_command(self, query_type, cmd_id):
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-TOKEN": self.ehc_token
        }
        cmd_body = {
            "command_setting": {
                "id": cmd_id
            }
        }

        return requests.post(url_path_join(self.api_url, "/api/ehc/command/", query_type),
                             headers=headers,
                             data=json.dumps(cmd_body))

    def _get_user(self):
        headers = {
            "X-AUTH-TOKEN": self.ehc_token
        }
        return requests.get(url_path_join(self.api_url, "/api/ehc/user"),
                            headers=headers)

    def create_shell_command(self, cmd):
        req_handler = "/api/ehc/command/submit"
        r = self._post_command(req_handler, "shell", inline=cmd)
        if r.status_code != 200:
            raise ServiceError("ServiceError", "Failed to request '%s'" % req_handler)
        return r.json()

    def create_hadoop_jar_command(self, jar_path, jar_args, main_class):
        req_handler = "/api/ehc/command/submit"

        kwargs = {
            "jar": jar_path,
            "class": main_class,
            "arg": jar_args
        }
        r = self._post_command(req_handler, "jar", **kwargs)

        if r.status_code != 200:
            print r.request.url
            print r.request.body
            print r.content
            raise ServiceError("ServiceError", "Failed to request '%s'" % req_handler)
        return r.json()

    def create_hive_command(self, hql_script_path):
        req_handler = "/api/ehc/command/submit"
        r = self._post_command(req_handler, "hive", file=hql_script_path)
        if r.status_code != 200:
            print r.request.url
            print r.request.body
            print r.content
            raise ServiceError("ServiceError", "Failed to request '%s'" % req_handler)
        return r.json()

    def get_command_status(self, cmd_id):
        r = self._get_command(query_type="status", cmd_id=cmd_id)
        if r.status_code != 200:
            raise ServiceError("ServiceError", "Failed to get command status")
        return r.json()

    def get_command_results(self, cmd_id):
        r = self._get_command(query_type="results", cmd_id=cmd_id)
        if r.status_code != 200:
            raise ServiceError("ServiceError", "Failed to get command results")
        return r.json()

    def get_user(self):
        r = self._get_user()
        if r.status_code != 200:
            raise ServiceError("ServiceError", "Failed to get user.")
        return r.json()

    def wait_command(self, cmd_id, interval=1, verbose=True):
        while True:
            r = self.get_command_status(cmd_id)
            job_status = r['status'].lower()
            if verbose:
                print "EHC.Command(ehc_id = '%s', cmd_id = '%s') Status is '%s'" % (self.ehc_id, cmd_id, job_status)
            if job_status in ["running", "prep"]:
                time.sleep(interval)
                continue
            return job_status


class BaseError(Exception):
    """
    General Ehc Client error
    """
    def __init__(self, reason, *args):
        super(BaseError, self).__init__(reason, *args)
        self.reason = reason

    def __repr__(self):
        return 'EhcClientError: %s' % self.reason

    def __str__(self):
        return 'EhcClientError: %s' % self.reason


class ServiceError(BaseError):
    """
    Error when request to EHC service.
    """
    def __init__(self, reason, message):
        super(ServiceError, self).__init__(reason, message)
        self.message = message

    def __repr__(self):
        return 'ServiceError: %s' % self.reason

    def __str__(self):
        return 'ServiceError: %s' % self.reason
