# -*- coding: utf-8 -*-

"""
snakemake utils
"""

import os

import configargparse
from snakemake.io import expand, glob_wildcards


def load_env(env_file: str = '.env'):
    """
    load .env file, and set as environment variables
    :param env_file: env filename (default ".env")
    """
    env_conf = configargparse.DefaultConfigFileParser()
    with open(env_file) as conf_file:
        envs = env_conf.parse(conf_file)
    envs.update(os.environ)  # don't overwrite original environment
    os.environ.update(envs)


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


def print_help(rules):
    """
    print targets and their docstrings as description
    :param rules: rules object
    """
    def is_private_rule(r):
        return r[0] == '_'

    print("\nDefined targets:")
    rule_dict = vars(rules)
    max_name_len = max(len(n) for n in rule_dict if not is_private_rule(n))
    for r_name, r_obj in rule_dict.items():
        doc_str = r_obj.rule.docstring
        if doc_str is None or is_private_rule(r_name):
            continue
        print(f'    {r_name:<{max_name_len}}    {doc_str}')
    print()
