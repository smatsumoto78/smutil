# -*- coding: utf-8 -*-

"""
Snakemake の lambda 実行に切り替える Rule デコレータ
"""

import inspect
import logging
from unittest.mock import patch

import snakemake.shell as shell
from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
S3 = S3RemoteProvider()

logger = logging.getLogger(__name__)


class patched_shell(shell):
    """shell() の置き換え。結局、rule の中にある shell(...) が呼び出されるのでパッチが必要"""
    def __new__(cls, cmd, *args, **kwargs):
        logger.debug("shell is replaced")
        logger.debug("  cmd: " + str(cmd))
        logger.debug("  args: " + str(args))
        logger.debug("  kwargs: " + str(kwargs))

        # 引数で渡ってくる cmd をデコレーション
        cmd = './lambda_invoker.sh  ' + cmd

        # 引数渡しの有効化 - snakemake.utils.format でスタックフレームを 2 つもどって変数を
        # 探しているので、それに対応できるようモック元の変数を取り込む
        current_frame = inspect.currentframe()
        prev_frame = current_frame.f_back
        globals().update(prev_frame.f_globals)
        locals().update(prev_frame.f_locals)

        # 元の shell() を呼び出し!!
        super().__new__(cls, cmd, *args, **kwargs)


def rule_lambda(prm=None):
    def decorate(ruleinfo):
        original_func = ruleinfo.func
        @patch('snakemake.workflow.shell', new=patched_shell)
        def new_func(*args, **kwargs):
            return original_func(*args, **kwargs)

        ruleinfo.func = new_func

        # Add 'lambda_invoker.sh' to call AWS lambda function
        ruleinfo.shellcmd = './lambda_invoker.sh ' + ruleinfo.shellcmd
        # ruleinfo.docstring += prm

        # Apply S3.remote(*. stay_on_remote=True) for all non-keywords of input and output.
        def s3_remote_(*args, **kwargs):
            return S3.remote(*args, stay_on_remote=True, **kwargs)

        ruleinfo.input = (tuple(map(s3_remote_, ruleinfo.input[0])),
                          ruleinfo.input[1])
        ruleinfo.output = (tuple(map(s3_remote_, ruleinfo.output[0])),
                           ruleinfo.output[1])

        return ruleinfo

    return decorate
