1) 4cpu-dtest-fleet - used for the machines doing the test selection (for full/strong/long suites)
2) strong-dtest-fleet - used for the running the tests

## create fleet

```bash
# example of how to create a fleet from the configuration
aws ec2 request-spot-fleet --region us-east-1 \
 --spot-fleet-request-config file://fedora-33-4cpu-dtest-us-east-1.json
```

## create ASGs

```bash
aws autoscaling create-auto-scaling-group --cli-input-json file://us-east-1-4cpu-dtest-asg-spot.json --region us-east-1
aws autoscaling create-auto-scaling-group --cli-input-json file://us-east-1-strong-dtest-asg.json --region us-east-1
aws autoscaling create-auto-scaling-group --cli-input-json file://us-east-1-strong-dtest-asg-spot.json --region us-east-1
```

## upload cloud-init script (if it changed)

```bash
aws s3 cp cloud-init-dtest-storage-only.sh  s3://downloads.scylladb.com/userData/cloud-init-dtest-storage-only.sh
aws s3 cp cloud-init-dtest.sh  s3://downloads.scylladb.com/userData/cloud-init-dtest.sh
```

## jenkins configuration

Go to https://jenkins.scylladb.com/configureClouds
Click "Add new Cloud" -> "AWS EC2 Fleet", to create a new configuration

And configure it as following:

* Name: DtestFleetCloud
* AWS Credentials: (jenkins2 aws account)
* Region: us-east-1 US East (N. Virginia)
* Endpoint like https://ec2.us-east-2.amazonaws.com
* EC2 Fleet: [select the id of the create fleet, from the dropdown]

* Launch agents via SSH
* Credentials: user-jenkins_scylla-qa-ec2.pem
* Host Key Verification Strategy: None verifying Verification Strategy

* Host Key Verification Strategy
* Label: ec2-fleet-strong-dtest
* Jenkins Filesystem Root: /jenkins/
* Number of Executors: 1
* Scale Executors by Weight?
* Max Idle Minutes Before Scaledown: 10
* Minimum Cluster Size: 0
* Maximum Cluster Size: 50
* Maximum Total Uses: -1
* Disable Build Resubmit: true
* Maximum Init Connection Timeout in sec: 600
* Cloud Status Interval in sec: 10
* No Delay Provision Strategy: false
