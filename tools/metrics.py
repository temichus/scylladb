import re
from typing import List

import requests


def _prometheus_get(ip, port='9180'):
    prometheus_url = f'http://{ip}:{port}/metrics'
    resp = requests.get(prometheus_url)
    resp.raise_for_status()
    return resp.text


def get_node_metrics(node_ip: str, metrics: List[str], port='9180'):
    metrics_res = {}
    filter_metrics = [metric for metric in _prometheus_get(node_ip, port).splitlines() if not metric.startswith('#')]
    for metric in filter_metrics:
        for metric_name in metrics:
            if re.search(metric_name, metric):
                val = metric.split()[-1]
                try:
                    val = int(val)
                except ValueError:
                    val = float(val)
                metrics_res[metric_name] = val if metric_name not in metrics_res \
                    else metrics_res[metric_name] + val
    return metrics_res
