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

from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
S3 = S3RemoteProvider()


def remote_s3(disable=False, **s3_remote_params):
    """
    Decorator for snakemake 'rule:'. Wrap "input:" and "output:" with S3.remote() call
    :param disable: Disable S3.remote wrapping. Default: False
    :param s3_remote_params: parameters passed for S3.remote()
    :return: Rule decorator
    """
    def decorate(ruleinfo):
        if disable:
            return ruleinfo  # if "disable" = True, do nothing

        # Apply S3.remote(*. keep_local=True) for all non-keywords of input and output.
        def s3_remote_(*args):
            return S3.remote(*args, **s3_remote_params)

        # Wrap 'input:' and 'output:' with S3.remote call
        if ruleinfo.input:
            ruleinfo.input = (tuple([s3_remote_(x) for x in ruleinfo.input[0]]),
                              {k: s3_remote_(v) for k, v in ruleinfo.input[1].items()})
        if ruleinfo.output:
            ruleinfo.output = (tuple([s3_remote_(x) for x in ruleinfo.output[0]]),
                               {k: s3_remote_(v) for k, v in ruleinfo.output[1].items()})

        return ruleinfo

    return decorate
