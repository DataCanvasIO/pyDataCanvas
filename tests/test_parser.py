
import os
import pytest
from datacanvas.clusters import EmrCluster
from datacanvas.log_parser import parse_s3_stderr, parse_syslog


@pytest.mark.xfail
def test_main():
    aws_region = os.getenv("AWS_DEFAULT_REGION")
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_sec = os.getenv("AWS_SECRET_ACCESS_KEY")
    job_id = os.getenv("TEST_JOB_ID")
    step_id = os.getenv("TEST_STEP_ID")

    emr_cluster = EmrCluster(aws_region, aws_key, aws_sec, job_id)
    assert emr_cluster

    print "================ stdout ==================="
    should_not_exist = parse_s3_stderr(emr_cluster.emr_step_log(step_id))
    assert not should_not_exist
    print "================ stderr ==================="
    print parse_s3_stderr(emr_cluster.emr_step_log(step_id, log_file="stderr"))
    assert parse_s3_stderr(emr_cluster.emr_step_log(step_id, log_file="stderr"))
    # print "================ syslog ==================="
    # should_not_exist = emr_cluster.get_step_log(step_id, log_file="syslog")
    # assert not should_not_exist


def my_main_local():
    log_path = os.path.join(os.path.dirname(__file__), 'testdata/emr_logs/test_stderr.log')
    p = parse_s3_stderr(open(log_path).read())
    # for g in p[0]["Groups"]:
    #     print g
    for gk, gv in p[0]["Groups"].asDict.items():
        print gk
        for k, v in gv.asDict.items():
            print "   %s => %s" % (k, v)

    # print "----------------------------------------------------"
    # print p[0]["Groups"][0].groupDict["Failed map tasks"]
    # print p[0]["Groups"].asDict.items()
    print "----------------------------------------------------"
    log_path = os.path.join(os.path.dirname(__file__), 'testdata/emr_logs/test_syslog.log')
    p = parse_syslog(open(log_path).read())

    for gk, gv in p[0]["Groups"].asDict.items():
        print gk
        for k, v in gv.asDict.items():
            print "   %s => %s" % (k, v)

    print "----------------------------------------------------"
    print p[0]["Groups"].asDict["Join_R_source_unknown"].asDict["unique_uuid_with_fbid"]
    assert True


def test_parser():
    assert True

if __name__ == "__main__":
    test_main()
