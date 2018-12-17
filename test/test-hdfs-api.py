from livy_submit import hdfs_api


def test_upload_and_delete(NAMENODE_URL, pi_file):
    hdfs_dirname = hdfs_api.upload(namenode_url=NAMENODE_URL,
                                   local_file=pi_file)
    hdfs_api.delete(hdfs_dir=hdfs_dirname, namenode_url=NAMENODE_URL)


