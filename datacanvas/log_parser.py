# -*- coding: utf-8 -*-

"""
A series of functions to parse hadoop logs, for example, the hadoop counter.
"""

import re
import string
import pyparsing as pyp
from collections import OrderedDict

try:
    from cStringIO import StringIO
except ImportError:  # python 3
    from io import StringIO


class SemanticGroup(object):
    """The (abstract) class to represent a semantic group of pyparsing."""

    label = "SemanticGroup"

    def __init__(self, contents):
        self.contents = contents
        while self.contents[-1].__class__ == self.__class__:
            self.contents = self.contents[:-1] + self.contents[-1].contents

    def __str__(self):
        return "%s(%s)" % (self.label,
                           " ".join([isinstance(c, basestring) and c or str(c) for c in self.contents]))

    def __repr__(self):
        return "%s(%s)" % (self.label,
                           " ".join([isinstance(c, basestring) and c or str(c) for c in self.contents]))


class HadoopCounter(SemanticGroup):
    """The class to represent a hadoop counter."""

    label = "HadoopCounter"

    def __init__(self, contents):
        super(HadoopCounter, self).__init__(contents)
        self.name = contents[0]
        self.val = contents[1]

    def __str__(self):
        return "<HadoopCounter :: '%s' = %s>" % (self.name, self.val)

    def __repr__(self):
        return "<HadoopCounter :: '%s' = %s>" % (self.name, self.val)


class HadoopCounterGroup(SemanticGroup):
    """The class to represent a hadoop counter group."""

    label = "HadoopCounterGroup"

    def __init__(self, contents):
        super(HadoopCounterGroup, self).__init__(contents)
        self.name = contents[0]
        self.group = contents[1:]

    def __str__(self):
        return "<HadoopCounterGroup :: '%s' => '%s'>" % (self.name, self.group)

    def __repr__(self):
        return "<HadoopCounterGroup :: '%s' => '%s'>" % (self.name, self.group)

    @property
    def asDict(self):
        """Return hadoop counter group as dict.

        :rtype : dict
        :return: group as dict.
        """
        return OrderedDict([(g.name, g.val) for g in self.group])


class HadoopCounterGroups(SemanticGroup):
    """The class to represent a hadoop counter groups."""

    label = "HadoopCounterGroup"

    def __init__(self, contents):
        super(HadoopCounterGroups, self).__init__(contents)
        self.groups = contents

    def __str__(self):
        return "<HadoopCounterGroups :: '%s'>" % self.groups

    def __repr__(self):
        return "<HadoopCounterGroups :: '%s'>" % self.groups

    @property
    def asDict(self):
        """Return hadoop counter groups as dict.

        :rtype : dict
        :return: groups as dict.
        """
        return OrderedDict([(g.name, g) for g in self.groups])


class SyslogMessage(SemanticGroup):
    """The class to represent a hadoop counter group."""

    label = "SyslogMessage"

    def __init__(self, contents):
        super(SyslogMessage, self).__init__(contents)
        self.msg_date = contents[0]
        self.level = contents[1]
        self.jar_pkg = contents[2]
        self.jar_main = contents[3]
        self.msg = contents[4]

    def __str__(self):
        return "<SyslogMessage :: '%s' '%s' '%s' '%s'>" % (self.msg_date, self.level, self.jar_pkg, self.jar_main)

    def __repr__(self):
        return "<SyslogMessage :: '%s' '%s' '%s' '%s'>" % (self.msg_date, self.level, self.jar_pkg, self.jar_main)


def makeGroupObject(cls):
    def groupAction(s, l, t):
        try:
            return cls(t[0].asList())
        except:
            return cls(t)

    return groupAction


# ##############################################
# Parse 'stderr' text from s3
###############################################


def syntax_hadoop_counter():
    """Build a grammer to parse hadoop counter from logs.

    :return: A pyparsing parser.
    """

    integer = pyp.Word(pyp.nums)
    words = pyp.Word(pyp.alphas + ".")
    words_ = pyp.Word(pyp.printables + " ", excludeChars="=")
    # header = pyp.Combine(words + ": " + words + ": " + integer)
    header = pyp.Literal("Counters:") + integer
    group_name = words_

    item_key = words_ + pyp.Suppress("=")
    item_value = integer
    counter_kv = pyp.Group(item_key + item_value).setParseAction(makeGroupObject(HadoopCounter))
    counter_group = pyp.Group(group_name + pyp.OneOrMore(counter_kv)) \
        .setParseAction(makeGroupObject(HadoopCounterGroup))
    counter_groups = pyp.Group(pyp.OneOrMore(counter_group))

    # bnf = header("Header") + counter_groups("Groups")
    bnf = header("Header") + counter_groups("Groups").setParseAction(makeGroupObject(HadoopCounterGroups))

    return bnf


SYSLOG_PATTERN = r"^\d{2}/\d{1,2}/\d{2,4} \d{2}:\d{2}:\d{2} [A-Z]* "


def extract_hadoop_counter(txt_block):
    """Extract hadoop counter from string of text block.

    :param txt_block: The string block to be processed.
    :return: A Hadoop group counter object.
    """
    hadoop_counter_parser = syntax_hadoop_counter()
    # r = hadoop_counter_parser.parseString(txt_block)
    r = hadoop_counter_parser.searchString(txt_block)
    return r


def parse_s3_stderr(content_text):
    """The function to parse hadoop log and extract hadoop counters.

    The format looks like this:

        ...
        14/10/22 08:08:22 INFO mapreduce.Job: Counters: 13
            Job Counters
                Failed map tasks=11
                Killed map tasks=2
                Launched map tasks=13
                Other local map tasks=9
                Rack-local map tasks=4
                Total time spent by all maps in occupied slots (ms)=871299
                Total time spent by all reduces in occupied slots (ms)=0
                Total time spent by all map tasks (ms)=290433
                Total vcore-seconds taken by all map tasks=290433
                Total megabyte-seconds taken by all map tasks=223052544
            Map-Reduce Framework
                CPU time spent (ms)=0
                Physical memory (bytes) snapshot=0
                Virtual memory (bytes) snapshot=0
        ...

    :param content_text: the string to be parsed
    :return: A Hadoop group counter object.
    """
    for l in re.split(SYSLOG_PATTERN, content_text, flags=re.MULTILINE):
        if re.match("^mapreduce.Job: Counters: \d+", l):
            return extract_hadoop_counter(l)


###############################################
# Parse syslog format
###############################################

def syntax_unified_log():
    msg_date = pyp.Regex("^\d{2,4}[-/]\d{1,2}[-/]\d{2,4} \d{2}:\d{2}:\d{2}(,\d+)?")
    level = pyp.Word(string.ascii_uppercase)
    jar_pkg = pyp.Regex("[a-zA-Z\.]+")
    jar_main = pyp.Suppress("(") + pyp.Word(pyp.printables, excludeChars="()") + pyp.Suppress(")")

    header = msg_date + level + jar_pkg + jar_main + pyp.Suppress(":")
    msg = pyp.restOfLine
    bnf = pyp.Group(header + msg).setParseAction(makeGroupObject(SyslogMessage))
    return bnf


def syslog_generator(texts, filter_condition=lambda x: True):
    p = syntax_unified_log()
    for l in StringIO(texts):
        obs = p.parseString(l)
        if len(obs) == 1:
            o = obs[0]
            if filter_condition(o):
                yield o


def parse_syslog(text):
    """This function will parse text like this:

        ...
        2014-11-19 16:04:54,677 INFO org.apache.hadoop.mapred.JobClient (main): Job complete: job_201411191552_0001
        2014-11-19 16:04:54,711 INFO org.apache.hadoop.mapred.JobClient (main): Counters: 40
        2014-11-19 16:04:54,711 INFO org.apache.hadoop.mapred.JobClient (main):   counter_group_name
        2014-11-19 16:04:54,711 INFO org.apache.hadoop.mapred.JobClient (main):     counter name 1=10
        2014-11-19 16:04:54,712 INFO org.apache.hadoop.mapred.JobClient (main):     counter name 2=10
        2014-11-19 16:04:54,712 INFO org.apache.hadoop.mapred.JobClient (main):   another_counter_group_name
        ...


    :param text: The string content to be parsed.
    :return: A Hadoop group counter object.
    """
    from itertools import dropwhile

    info_texts = syslog_generator(text, lambda x: x.level in ["INFO"])
    counter_text_block = [l.msg for l in dropwhile(lambda x: not re.search("Job complete: \w+", x.msg), info_texts)]
    return extract_hadoop_counter("\n".join(counter_text_block))
