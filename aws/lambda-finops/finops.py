import boto3
import os
import urllib.request
import urllib.error
import concurrent.futures
import logging
from botocore.config import Config

# -----------------------------
# LOGGING & CONFIG
# -----------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

PUSHGATEWAY = os.environ["PUSHGATEWAY_URL"]
JOB_NAME = os.environ.get("JOB_NAME", "aws_idle_resources")

# Optimize retries for Lambda (fail fast)
config = Config(
    retries={"max_attempts": 3, "mode": "standard"},
    read_timeout=10,
    connect_timeout=5
)

# Global session for connection pooling
session = boto3.Session()

# -----------------------------
# PROM FORMAT HELPER
# -----------------------------
def prom_metric(name, labels, value):
    label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
    return f'{name}{{{label_str}}} {value}\n'

# -----------------------------
# REGION DISCOVERY
# -----------------------------
def get_all_regions():
    # Use global client just for this call
    ec2_global = session.client("ec2", region_name="us-east-1", config=config)
    response = ec2_global.describe_regions(AllRegions=False)
    return [r["RegionName"] for r in response["Regions"]]

# -----------------------------
# COLLECTORS
# -----------------------------
def collect_idle_ebs(ec2, region):
    metrics = []
    paginator = ec2.get_paginator("describe_volumes")
    
    # "available" means unattached (idle cost)
    iterator = paginator.paginate(Filters=[{"Name": "status", "Values": ["available"]}])
    
    for page in iterator:
        for v in page["Volumes"]:
            metrics.append(prom_metric(
                "aws_idle_ebs_volume",
                {
                    "volume_id": v["VolumeId"],
                    "az": v["AvailabilityZone"],
                    "size_gb": str(v["Size"]),
                    "region": region,
                },
                1
            ))
    return metrics

def collect_unused_eip(ec2, region):
    metrics = []
    addresses = ec2.describe_addresses()["Addresses"]
    
    for a in addresses:
        # If no AssociationId, it is not attached to any interface
        if "AssociationId" not in a:
            metrics.append(prom_metric(
                "aws_idle_eip",
                {
                    "allocation_id": a["AllocationId"],
                    "public_ip": a["PublicIp"],
                    "region": region,
                },
                1
            ))
    return metrics

def collect_idle_nat(ec2, region):
    metrics = []
    paginator = ec2.get_paginator("describe_nat_gateways")
    
    # NOTE: "Available" just means it exists. To truly detect IDLE NATs, 
    # you would need to query CloudWatch metrics (BytesOut < threshold).
    # This currently alerts on ALL active NAT Gateways.
    for page in paginator.paginate():
        for nat in page["NatGateways"]:
            if nat["State"] == "available":
                metrics.append(prom_metric(
                    "aws_idle_nat_gateway",
                    {
                        "nat_id": nat["NatGatewayId"],
                        "vpc_id": nat["VpcId"],
                        "subnet_id": nat["SubnetId"],
                        "region": region,
                    },
                    1
                ))
    return metrics

def collect_stopped_ec2(ec2, region):
    metrics = []
    paginator = ec2.get_paginator("describe_instances")
    
    iterator = paginator.paginate(Filters=[{"Name": "instance-state-name", "Values": ["stopped"]}])
    
    for page in iterator:
        for r in page["Reservations"]:
            for i in r["Instances"]:
                metrics.append(prom_metric(
                    "aws_stopped_ec2_instance",
                    {
                        "instance_id": i["InstanceId"],
                        "instance_type": i["InstanceType"],
                        "region": region,
                    },
                    1
                ))
    return metrics

# -----------------------------
# NETWORK OPERATIONS
# -----------------------------
def delete_previous_metrics(region):
    """Clean up old metrics for this region to prevent stale data."""
    delete_url = f"{PUSHGATEWAY}/metrics/job/{JOB_NAME}/region/{region}"
    req = urllib.request.Request(url=delete_url, method="DELETE")

    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status not in (202, 204):
                logger.warning(f"Delete failed for {region}: {resp.status}")
    except urllib.error.HTTPError as e:
        if e.code != 404: # 404 is fine (no previous metrics)
            logger.warning(f"Delete HTTP error {region}: {e}")

def push_metrics(region, metrics_data):
    url = f"{PUSHGATEWAY}/metrics/job/{JOB_NAME}/region/{region}"
    req = urllib.request.Request(
        url=url,
        data=metrics_data.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "text/plain"}
    )
    
    with urllib.request.urlopen(req, timeout=5) as resp:
        if resp.status >= 300:
            raise Exception(f"Pushgateway error {resp.status}")

# -----------------------------
# REGION WORKER
# -----------------------------
def process_region(region):
    """Orchestrates collection and push for a single region."""
    try:
        # Create client specific to this thread/region
        ec2 = session.client("ec2", region_name=region, config=config)

        # 1. Clear old state
        delete_previous_metrics(region)

        # 2. Collect all metrics (List append is faster than string concatenation)
        payload_list = []
        payload_list.extend(collect_idle_ebs(ec2, region))
        payload_list.extend(collect_unused_eip(ec2, region))
        payload_list.extend(collect_idle_nat(ec2, region))
        payload_list.extend(collect_stopped_ec2(ec2, region))

        # 3. Push if data exists
        if payload_list:
            final_payload = "".join(payload_list)
            push_metrics(region, final_payload)
            return len(payload_list)
        
        return 0

    except Exception as e:
        logger.error(f"Failed to process region {region}: {str(e)}")
        return 0

# -----------------------------
# LAMBDA ENTRYPOINT
# -----------------------------
def lambda_handler(event, context):
    try:
        regions = get_all_regions()
        logger.info(f"Scanning {len(regions)} regions...")
        
        total_metrics = 0
        
        # Parallel Execution
        # Max workers = 10-20 is usually safe for Lambda memory limits
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_region = {executor.submit(process_region, r): r for r in regions}
            
            for future in concurrent.futures.as_completed(future_to_region):
                total_metrics += future.result()

        return {
            "status": "ok",
            "regions_scanned": len(regions),
            "metrics_sent": total_metrics
        }
        
    except Exception as e:
        logger.error(f"Critical failure: {str(e)}")
        raise