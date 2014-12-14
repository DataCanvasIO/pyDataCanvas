
import os
from datacanvas.clusters import EmrCluster


def test_upload():
    print "Hello"
    aws_region = os.getenv("AWS_DEFAULT_REGION")
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_sec = os.getenv("AWS_SECRET_ACCESS_KEY")
    job_id = os.getenv("TEST_JOB_ID")
    step_id = os.getenv("TEST_STEP_ID")

    emr_cluster = EmrCluster(aws_region, aws_key, aws_sec, job_id)
    # print emr_cluster.s3_get_file("s3://xiaolin")
    # print emr_cluster.s3_get_file("s3://xiaolin/")
    # print emr_cluster.s3_get_file("s3://xiaolin/zetjob/")
    print emr_cluster.s3_list_files("s3://xiaolin/zetjob/")

    # src_path = os.path.join(os.path.dirname(__file__), "testdata/emr_runtime/spec.json")
    # # os.chdir(os.path.dirname(__file__))
    # # src_path = os.path.join("testdata/")
    # dst_path = "s3://xiaolin/runtime_test/"
    # emr_cluster.s3_upload(src_path, dst_path, recursive=True)


if __name__ == "__main__":
    test_upload()
