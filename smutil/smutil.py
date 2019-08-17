# -*- coding: utf-8 -*-


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
