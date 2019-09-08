# -*- coding: utf-8 -*-

"""
Snakemake からの lambda 実行用実装のヘルパ
"""

import contextlib
import logging
import os
import shlex
import sys

import boto3

logger = logging.getLogger(__name__)
s3 = boto3.client('s3')
is_lambda = False


def add_lambda_handler(func):
    # lambda 対応 - /tmp に行って、event["ARGS"] を引数として使われるようにする
    def lambda_handler(event, context):
        os.chdir('/tmp')
        args_str = os.getenv("ARGS", "") + " " + event["ARGS"]
        args_list = shlex.split(args_str)

        global is_lambda
        is_lambda = True
        func(args_list)
        is_lambda = False

    # lambda_handler を追加
    m = sys.modules[func.__module__]
    setattr(m, 'lambda_handler', lambda_handler)

    return func


@contextlib.contextmanager
def lambda_s3_prepare(input_filename, output_filename):
    """S3からのダウンロード・アップロードを実施する context"""
    is_s3 = is_use_s3(input_filename)
    if is_s3:
        s3_download(s3, input_filename)

    yield

    if is_s3:
        s3_upload(s3, output_filename)


def is_use_s3(filename):
    """S3 へのダウンロード・アップロードを実行するか否かの判定を返す"""
    logger.info(is_lambda)
    return is_lambda


def s3_download(s3, input_filename):
    """
    指定されたファイルをダウンロード。
    ファイル名は [s3://]bucket_name/key_name という前提 (snakemake のルールと同じ)
    """
    logger.info('S3 download!')
    input_filename = input_filename.replace('s3://', '')
    input_path, _ = os.path.split(input_filename)
    os.makedirs(input_path, exist_ok=True)
    input_bucket, input_key = input_filename.split('/', 1)
    s3.download_file(input_bucket, input_key, input_filename)


def s3_upload(s3, output_filename):
    """
    指定されたファイルをアップロード。
    ファイル名は [s3://]bucket_name/key_name という前提 (snakemake のルールと同じ)
    """
    logger.info('S3 upload!')
    output_filename = output_filename.replace('s3://', '')
    output_bucket, output_key = output_filename.split('/', 1)
    s3.upload_file(output_filename, output_bucket, output_key)
