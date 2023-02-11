from enum import Enum, IntEnum, auto


class DataPathStrategyEnum(Enum):
    FAIL_FAST = 1
    WAIT_FINISHED = 2
    WAIT_UNTIL_THEN_GO = 3


class GitOperatorEnum(Enum):
    CLONE_ALL = 1
    CLONE_PARTIAL = 2
    CLONE_SUBMODULE_ONLY = 3
    CLONE_ALL_WITH_SUBMODULE = 4


class ConfigType(Enum):
    # support configparser
    INI = 1
    YAML = 2


class DynamicDagState(Enum):
    # def _generate_next_value_(name, start, count, last_values):
    #     """generate consecutive automatic numbers starting from zero"""
    #     return count

    ADDING = 0
    ADD_TIMEOUT = 1
    ADD_ERROR = 2
    ADDED = 3
    UPDATING = 4
    UPDATE_TIMEOUT = 5
    UPDATE_ERROR = 6
    UPDATED = 7
    DELETING = 8
    DELETE_TIMEOUT = 9
    DELETE_ERROR = 10
    DELETED = 11
    RUNNING = 12
    RUN_TIMEOUT = 13
    RUN_FAILED = 14
    RUN_SUCCEED = 15
    PAUSED = 16
    DISABLE = 17
    ENABLE = 18
