#!/usr/bin/python

import re
import time
import requests
import argparse
from pprint import pprint

import os
from sys import exit
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY

DEBUG = int(os.environ.get('DEBUG', '0'))


class ResourceManagerCollector(object):
    # The build statuses we want to export about.
    statuses = {
        "appsSubmitted": "The number of applications submitted",
        "appsCompleted": "The number of applications completed",
        "appsPending": "The number of applications pending",
        "appsRunning": "The number of applications running",
        "appsFailed": "The number of applications failed",
        "appsKilled": "The number of applications killed",
        "reservedMB": "The amount of memory reserved in MB",
        "availableMB": "The amount of memory available in MB",
        "allocatedMB": "The amount of memory allocated in MB",
        "reservedVirtualCores": "The number of reserved virtual cores",
        "availableVirtualCores": "The number of available virtual cores",
        "allocatedVirtualCores": "The number of allocated virtual cores",
        "containersAllocated": "The number of containers allocated",
        "containersReserved": "The number of containers reserved",
        "containersPending": "The number of containers pending",
        "totalMB": "The amount of total memory in MB",
        "totalVirtualCores": "The total number of virtual cores",
        "totalNodes": "The total number of nodes",
        "lostNodes": "The number of lost nodes",
        "unhealthyNodes": "The number of unhealthy nodes",
        "decommissionedNodes": "The number of nodes decommissioned",
        "rebootedNodes": "The number of nodes rebooted",
        "activeNodes": "The number of active nodes"
    }

    def __init__(self, target, cluster):
        self._cluster = cluster
        self._target = target.rstrip("/")
        self._prefix = 'hadoop_resourcemanager_'

    def collect(self):
        # Request data from resourcemanager API
        clusterMetrics = self._request_data()

        self._setup_empty_prometheus_metrics()

        self._get_metrics(clusterMetrics)

        for status in self.statuses:
            yield self._prometheus_metrics[status]

    def _request_data(self):
        # Request exactly the information we need from resourcemanager
        url = '{0}/ws/v1/cluster/metrics'.format(self._target)

        def parsejobs(myurl):
            response = requests.get(myurl) #, params=params, auth=(self._user, self._password))
            if response.status_code != requests.codes.ok:
                return[]
            result = response.json()
            if DEBUG:
                pprint(result)

            return result

        '''
        {
        "clusterMetrics":{
            "appsSubmitted": 74350,
            "appsCompleted": 74322,
            "appsPending": 0,
            "appsRunning": 10,
            "appsFailed": 2,
            "appsKilled": 16,
            "reservedMB": 515072,
            "availableMB": 493568,
            "allocatedMB": 2168832,
            "reservedVirtualCores": 108,
            "availableVirtualCores": 848,
            "allocatedVirtualCores": 452,
            "containersAllocated": 452,
            "containersReserved": 108,
            "containersPending": 53536,
            "totalMB": 2662400,
            "totalVirtualCores": 1300,
            "totalNodes": 65,
            "lostNodes": 0,
            "unhealthyNodes": 0,
            "decommissionedNodes": 0,
            "rebootedNodes": 0,
            "activeNodes": 65
        }
        }
        '''

        return parsejobs(url)['clusterMetrics']

    def _setup_empty_prometheus_metrics(self):
        # The metrics we want to export.
        self._prometheus_metrics = {}
        for status in self.statuses:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', status).lower()
            self._prometheus_metrics[status] = GaugeMetricFamily(self._prefix + snake_case,
                                      self.statuses[status], labels=["cluster"])

    def _get_metrics(self, clusterMetrics):
        for status in self.statuses:
            self._prometheus_metrics[status].add_metric([self._cluster], clusterMetrics[status])


class ResourceManagerNodeCollector(object):
    # The build statuses we want to export about.
    statuses = {
        "state": "State of the node - valid values are: NEW, RUNNING, UNHEALTHY, DECOMMISSIONED, LOST, REBOOTED",
        "numContainers": "The total number of containers currently running on the node",
        "usedMemoryMB": "The total amount of memory currently used on the node (in MB)",
        "availMemoryMB": "The total amount of memory currently available on the node (in MB)",
        "usedVirtualCores": "The total number of vCores currently used on the node",
        "availableVirtualCores": "The total number of vCores available on the node",
    }

    NODE_STATE = {
        'NEW': 1, 
        'RUNNING': 2, 
        'UNHEALTHY': 3, 
        'DECOMMISSIONED': 4, 
        'LOST': 5, 
        'REBOOTED': 6,
    }

    def __init__(self, target, cluster):
        self._cluster = cluster
        self._target = target.rstrip("/")
        self._prefix = 'hadoop_resourcemanager_node_'

    def collect(self):
        # Request data from resourcemanager API
        nodeInfos = self._request_data()

        self._setup_empty_prometheus_metrics()

        for nodeInfo in nodeInfos:
            self._get_metrics(nodeInfo)

        for status in self.statuses:
            yield self._prometheus_metrics[status]

    def _request_data(self):
        # Request exactly the information we need from resourcemanager
        url = '{0}/ws/v1/cluster/nodes'.format(self._target)

        def parsejobs(myurl):
            response = requests.get(myurl) #, params=params, auth=(self._user, self._password))
            if response.status_code != requests.codes.ok:
                return[]
            result = response.json()
            if DEBUG:
                pprint(result)

            return result['nodes']['node']

        '''
        {
            "nodes":{
                "node":[
                    {
                    "rack": "/default-rack",
                    "state": "RUNNING",
                    "id": "bc3.clicki.cn:21642",
                    "nodeHostName": "bc3.clicki.cn",
                    "nodeHTTPAddress": "bc3.clicki.cn:8042",
                    "lastHealthUpdate": 1498805490510,
                    "version": "2.5.2",
                    "healthReport": "",
                    "numContainers": 3,
                    "usedMemoryMB": 6144,
                    "availMemoryMB": 34816,
                    "usedVirtualCores": 3,
                    "availableVirtualCores": 17
                    }
                ]
            }
        }
        '''

        return parsejobs(url)

    def _setup_empty_prometheus_metrics(self):
        # The metrics we want to export.
        self._prometheus_metrics = {}
        for status in self.statuses:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', status).lower()
            self._prometheus_metrics[status] = GaugeMetricFamily(self._prefix + snake_case,
                                      self.statuses[status], labels=["cluster", "host", "version"])

    def _get_metrics(self, nodeInfo):
        for status in self.statuses:
            if status == 'state':
                v = self.NODE_STATE[nodeInfo['state']]
            else:
                v = nodeInfo[status]
            self._prometheus_metrics[status].add_metric(
                [self._cluster, nodeInfo['nodeHostName'], nodeInfo['version']], v)



def parse_args():
    parser = argparse.ArgumentParser(
        description='resourcemanager exporter args resourcemanager address and port'
    )
    parser.add_argument(
        '-url', '--resourcemanager.url',
        metavar='url',
        dest='url',
        required=False,
        help='Hadoop ResourceManager URL. (default "http://localhost:8088")',
        default='http://localhost:8088'
    )
    parser.add_argument(
        '--telemetry-path',
        metavar='telemetry_path',
        required=False,
        help='Path under which to expose metrics. (default "/metrics")',
        default='/metrics'
    )
    parser.add_argument(
        '-p', '--port',
        metavar='port',
        required=False,
        type=int,
        help='Listen to this port. (default ":9088")',
        default=int(os.environ.get('VIRTUAL_PORT', '9088'))
    )
    parser.add_argument(
        '--cluster',
        metavar='cluster',
        required=True,
        help='label for cluster'
    )
    return parser.parse_args()


def main():
    try:
        args = parse_args()
        port = int(args.port)
        REGISTRY.register(ResourceManagerCollector(args.url, args.cluster))
        REGISTRY.register(ResourceManagerNodeCollector(args.url, args.cluster))
        
        start_http_server(port)
        print "Polling %s. Serving at port: %s" % (args.url, port)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(" Interrupted")
        exit(0)


if __name__ == "__main__":
    main()