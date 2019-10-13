# -*- coding: utf-8 -*-

"""
Snakemake の lambda 実行に切り替える Rule デコレータ
"""

import inspect
from unittest.mock import patch, MagicMock

from snakemake.workflow import RuleInfo
import snakemake
import snakemake.shell as shell

from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
S3 = S3RemoteProvider()


class patched_shell(shell):
    """shell() の置き換え。結局、rule の中にある shell(...) が呼び出されるのでパッチが必要"""
    def __new__(cls, cmd, *args, **kwargs):
        print("shell is replaced")
        print(args, kwargs)
        print(type(cls))
        print(cls)

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


def deco2(prm=None):
    def decorate(ruleinfo):
        original_func = ruleinfo.func
        @patch('snakemake.workflow.shell', new=patched_shell)
        def new_func(*args, **kwargs):
            return original_func(*args, **kwargs)

        ruleinfo.func = new_func

        ruleinfo.shellcmd = './lambda_invoker.sh ' + ruleinfo.shellcmd
        # ruleinfo.docstring += prm

        # Apply S3.remote() for all non-keywords of input and output.
        ruleinfo.input = (tuple(map(S3.remote, ruleinfo.input[0])),
                          ruleinfo.input[1])
        ruleinfo.output = (tuple(map(S3.remote, ruleinfo.output[0])),
                           ruleinfo.output[1])

        return ruleinfo

    return decorate
