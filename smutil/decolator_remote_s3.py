#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Decorator for snakemake 'rule:' - Wrap "input:" and "output:" with S3.remote() call

usage:
```
from smutil.decolator_remote_s3 import remote_s3

@remote_s3()
rule some_rule:
    input: 'some-bucket/input.txt'
    output: 'some-bucket/output.txt'
    shell: 'cat {input} > {output}'
```

"""

import os

from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
S3 = S3RemoteProvider()


def s3_remote_wrapper(disable=False, bucket='', *args, **s3_remote_params):
    """

    :param bucket:
    :param args:
    :param s3_remote_params:
    :return:
    """
    if disable:
        return args  # if "disable" = True, do nothing
    # if expand() is applied, the item in args is list, otherwise it is str.
    args_with_bucket = [[os.path.join(bucket, i) for i in x] if isinstance(x, list) else os.path.join(bucket, x)
                        for x in args]
    return S3.remote(*args_with_bucket, **s3_remote_params)


def remote_s3(disable=False, bucket='', **s3_remote_params):
    """
    Decorator for snakemake 'rule:'. Wrap "input:" and "output:" with S3.remote() call
    :param disable: Disable S3.remote wrapping. Default: False
    :param bucket: bucket name automatically added. Default: '' (no buckect name added, assumed "top folder" is bucket name same as S3.
    :param s3_remote_params: parameters passed for S3.remote()
    :return: Rule decorator
    """
    def decorate(ruleinfo):
        if disable:
            return ruleinfo  # if "disable" = True, do nothing

        # Wrap 'input:' and 'output:' with S3.remote call
        if ruleinfo.input:
            ruleinfo.input = (tuple([s3_remote_wrapper(bucket, x, **s3_remote_params) for x in ruleinfo.input[0]]),
                              {k: s3_remote_wrapper(bucket, v, **s3_remote_params) for k, v in ruleinfo.input[1].items()})
        if ruleinfo.output:
            ruleinfo.output = (tuple([s3_remote_wrapper(bucket, x, **s3_remote_params) for x in ruleinfo.output[0]]),
                               {k: s3_remote_wrapper(bucket, v, **s3_remote_params) for k, v in ruleinfo.output[1].items()})

        return ruleinfo

    return decorate
