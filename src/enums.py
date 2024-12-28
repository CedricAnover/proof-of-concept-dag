from enum import Enum


class NodeStateEnum(Enum):
    IDLE = 1
    RUNNING = 2
    COMPLETE_SUCCESS = 3
    COMPLETE_FAIL = 4
