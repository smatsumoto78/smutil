import smutil as util

rule hoge:
    """hoge rule description ふが"""
    input: "input-{sample}.txt"
    output: "output-{sample}.txt"
    shell: "myscript.sh {input} > {output}"

rule _huga:
    """just print huga"""
    shell: "echo huga"

rule help:
    """print targets and their descriptions"""
    run:
        util.print_help(rules)
