# -*- coding: utf-8 -*-

"""
snakemake utils
"""
import io
import os
import shlex
import subprocess
from typing import Union, Iterable

import configargparse
from snakemake.io import expand, glob_wildcards, touch


def load_env(env_file: str = '.env', sh_exp: bool = True):
    """
    load .env file, and set as environment variables
    :param env_file: env filename (default ".env")
    :param sh_exp: apply sh expansion (default True)
    """
    env_conf = configargparse.DefaultConfigFileParser()
    if sh_exp:
        sh_result = subprocess.run(["sh", "-x", env_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='ascii')
        env_str = sh_result.stderr.replace("+ ", "")
        sio = io.StringIO("\n".join(shlex.split(env_str)))
        envs = env_conf.parse(sio)
    else:
        with open(env_file) as conf_file:
            envs = env_conf.parse(conf_file)
    envs.update(os.environ)  # don't overwrite original environment
    os.environ.update(envs)


def touch_o(filename: Union[str, Iterable[str]]):
    """
    touch files within '.snakemake/touchfiles' folder

    usage:
    import smutil as util
    params = ['A', 'B', 'C']

    rule all:
        input: util.touch_i(expand('s_1_{p}', p=params)), util.touch_i('s_2')

    rule s_1:
        output: util.touch_o('s_1_{p}')
        shell: "echo s_1 {wildcards.p}"

    rule s_2:
        output: util.touch_o('s_2')
        shell: "echo s_2"

    """
    return touch(touch_i(filename))


def touch_i(filename: Union[str, Iterable[str]]):
    """
    return touch_in filename (to use in input: parameter)

    usage:
    import smutil as util
    params = ['A', 'B', 'C']

    rule all:
        input: util.touch_i(expand('s_1_{p}', p=params)), util.touch_i('s_2')

    rule s_1:
        output: util.touch_o('s_1_{p}')
        shell: "echo s_1 {wildcards.p}"

    rule s_2:
        output: util.touch_o('s_2')
        shell: "echo s_2"

    """
    if isinstance(filename, str):
        return os.path.join('.snakemake', 'touchfiles', filename)
    elif isinstance(filename, Iterable):
        return [touch_i(f) for f in filename]
    else:
        raise TypeError()


def aggregate_input_func(base_checkpoint_obj, base_rule, target_rule=None, output_key=0):
    """
    return input function for aggregating inputs from checkpoint
    :param base_checkpoint_obj: checkpoint object. "checkpoints.some_checkpoint"
    :param base_rule: aggregation filenames at checkpoint
    :param target_rule: aggregation filenames at previous rule. If previous rule is base_checkpoint, set None (default: None)
    :param output_key: key of checkpoint's output for aggregation directory. (default: 0)
    :return: function for input section
    """
    def aggregate_input(wildcards):
        ops = base_checkpoint_obj.get(**wildcards).output
        checkpoint_output = _output_accessor(ops, output_key)
        expand_base_rule = os.path.join(checkpoint_output, base_rule)
        expand_target_rule = target_rule or expand_base_rule
        return expand(expand_target_rule,
                      **glob_wildcards(expand_base_rule)._asdict())
    return aggregate_input


def _output_accessor(output, output_key):
    """
    access output member with output_key(int, str or callable)
    :param output:
    :param output_key:
    :return:
    """
    if isinstance(output_key, int):
        return output[output_key]
    elif isinstance(output_key, str):
        return getattr(output, output_key)
    else:
        # assume callable
        return output_key(output)
