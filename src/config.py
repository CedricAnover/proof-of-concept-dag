import json
from abc import ABC, abstractmethod
from typing import Any, Type, TypeVar, Optional

from pydantic import BaseModel


T = TypeVar('T', bound="Config")


class Config(ABC, BaseModel):
    def to_json(self, *args, **kwargs) -> str:
        # Config --> JSON String
        return self.model_dump_json(*args, **kwargs)

    @classmethod
    def from_json(cls, json_str: str, *args, **kwargs) -> Type[T]:
        # JSON String --> Config
        return cls.model_validate_json(json_str, *args, **kwargs)

    def to_yaml(self, *args, **kwargs) -> str:
        # Config --> YAML String
        # TODO: Add implementation to Config.to_yaml
        raise NotImplementedError("Add implementation to Config.to_yaml")

    @classmethod
    def from_yaml(self, yaml_str: str, *args, **kwargs) -> Type[T]:
        # YAML String --> Config
        # TODO: Add implementation to Config.from_yaml
        raise NotImplementedError("Add implementation to Config.from_yaml")

    @classmethod
    @abstractmethod
    def from_object(self, obj: Any, *args, **kwargs) -> Type[T]:
        #  Object --> Config
        pass

    @abstractmethod
    def to_object(self, *args, **kwargs) -> Any:
        # Config --> Object
        pass
