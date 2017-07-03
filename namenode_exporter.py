#!/usr/bin/python

import re
import time
import requests
import argparse
from pprint import pprint

import json
import os
from sys import exit
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY

DEBUG = int(os.environ.get('DEBUG', '0'))


class NameNodeCollector(object):
    # The build statuses we want to export about.
    statuses = {
        "MissingBlocks": "MissingBlocks",
        "CapacityTotal": "CapacityTotal",
        # "CapacityTotalGB": "CapacityTotalGB",
        "CapacityUsed": "CapacityUsed",
        # "CapacityUsedGB": "CapacityUsedGB",
        "CapacityRemaining": "CapacityRemaining",
        # "CapacityRemainingGB": "CapacityRemainingGB",
        "CapacityUsedNonDFS": "CapacityUsedNonDFS",
        "TotalLoad": "TotalLoad",
        "BlocksTotal": "BlocksTotal",
        "FilesTotal": "FilesTotal",
        "PendingReplicationBlocks": "PendingReplicationBlocks",
        "UnderReplicatedBlocks": "UnderReplicatedBlocks",
        "CorruptBlocks": "CorruptBlocks",
        "ScheduledReplicationBlocks": "ScheduledReplicationBlocks",
        "PendingDeletionBlocks": "PendingDeletionBlocks",
        "ExcessBlocks": "ExcessBlocks",
        "PostponedMisreplicatedBlocks": "PostponedMisreplicatedBlocks",
        "PendingDataNodeMessageCount": "PendingDataNodeMessageCount",
        "BlockCapacity": "BlockCapacity",
        "StaleDataNodes": "StaleDataNodes",
        "NumLiveDataNodes": "NumLiveDataNodes",
        "NumDeadDataNodes": "NumDeadDataNodes",
    }

    datanode_statuses = {
        "up": "node status. 1:up, 0:down",
        "blockPoolUsed": "blockPoolUsed",
        "blockPoolUsedPercent": "blockPoolUsedPercent",
        "capacity": "capacity, total space",
        "lastContact": "lastContact",
        "nonDfsUsedSpace": "nonDfsUsedSpace",
        "numBlocks": "numBlocks",
        "remaining": "remaining space",
        "used": "used space",
        # "usedSpace": "usedSpace", # same as used
    }

    def __init__(self, target, cluster):
        self._cluster = cluster
        self._target = target.rstrip("/")
        self._prefix = 'hadoop_namenode_'
        self._datanode_prefix = 'hadoop_datanode_node_'

    def collect(self):
        # Request data from namenode jmx API
        beans = self._request_data()

        self._setup_empty_prometheus_metrics()

        self._get_metrics(beans)

        for status in self.statuses:
            yield self._prometheus_metrics[status]

        for status in self.datanode_statuses:
            yield self._prometheus_datanode_metrics[status]
            

    def _request_data(self):
        # Request exactly the information we need from namenode
        # url = '{0}/jmx'.format(self._target)
        url = self._target

        def parsejobs(myurl):
            response = requests.get(myurl) #, params=params, auth=(self._user, self._password))
            if response.status_code != requests.codes.ok:
                return[]
            result = response.json()
            if DEBUG:
                pprint(result)

            return result['beans']

        return parsejobs(url)

    def _setup_empty_prometheus_metrics(self):
        # The metrics we want to export.
        self._prometheus_metrics = {}
        self._prometheus_datanode_metrics = {}
        for status in self.statuses:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', status).lower()
            self._prometheus_metrics[status] = GaugeMetricFamily(self._prefix + snake_case,
                                      self.statuses[status], labels=["cluster"])
        
        for status in self.datanode_statuses:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', status).lower()
            self._prometheus_datanode_metrics[status] = GaugeMetricFamily(self._datanode_prefix + snake_case,
                        self.datanode_statuses[status], labels=["cluster", "host", "xferaddr"])

    def _get_metrics(self, beans):
        for bean in beans:
            if bean['name'] == "Hadoop:service=NameNode,name=FSNamesystemState":
                for status in self.statuses:
                    if bean.has_key(status):
                        self._prometheus_metrics[status].add_metric([self._cluster], bean[status])
            if bean['name'] == "Hadoop:service=NameNode,name=FSNamesystem":
                for status in self.statuses:
                    if bean.has_key(status):
                        self._prometheus_metrics[status].add_metric([self._cluster], bean[status])
            elif bean['name'] == "Hadoop:service=NameNode,name=NameNodeInfo":
                liveNodes = json.loads(bean['LiveNodes'])
                deadNodes = json.loads(bean['DeadNodes'])
                for host in liveNodes:
                    node = liveNodes[host]
                    node['up'] = 1
                    for status in self.datanode_statuses:
                        self._prometheus_datanode_metrics[status].add_metric(
                            [self._cluster, host, node['xferaddr'] ], node[status])
                for host in deadNodes:
                    node = deadNodes[host]
                    node['up'] = 0
                    for status in self.datanode_statuses:
                        if node.has_key(status):
                            self._prometheus_datanode_metrics[status].add_metric(
                                [self._cluster, host, node['xferaddr'] ], node[status])





def parse_args():
    parser = argparse.ArgumentParser(
        description='namenode exporter args namenode address and port'
    )
    parser.add_argument(
        '-url', '--namenode.jmx.url',
        metavar='url',
        dest='url',
        required=False,
        help='Hadoop NameNode JMX URL. (default "http://localhost:50070/jmx")',
        default='http://localhost:50070/jmx'
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
        REGISTRY.register(NameNodeCollector(args.url, args.cluster))
        # REGISTRY.register(ResourceManagerNodeCollector(args.url, args.cluster))
        
        start_http_server(port)
        print "Polling %s. Serving at port: %s" % (args.url, port)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(" Interrupted")
        exit(0)


if __name__ == "__main__":
    main()