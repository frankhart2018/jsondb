import json
import os
import pickle
from typing import Optional
import uuid


def perform_op(left_operand: any, right_operand: any, operator: str) -> bool:
    assert type(left_operand) == type(right_operand), "Type mismatch!"
    output = []

    left_operand = f'"{left_operand}"' if type(left_operand) == str else left_operand
    right_operand = (
        f'"{right_operand}"' if type(right_operand) == str else right_operand
    )
    exec(f"output.append({left_operand} {operator} {right_operand})")

    if isinstance(output[0], bool):
        return output[0]
    else:
        return output[0] is not None


CONDITION_MAP = {
    "eq": "==",
    "neq": "!=",
    "lt": "<",
    "gt": ">",
    "lte": "<=",
    "gte": ">=",
    "all": "+",
}


class JsonDb:
    def __init__(
        self,
        file_path: str,
        add_id: Optional[bool] = False,
        overwrite: Optional[bool] = False,
    ) -> None:
        dir_name = os.path.dirname(file_path)
        if not os.path.exists(os.path.abspath(dir_name)):
            raise ValueError(f"Directory {dir_name} does not exist!")

        self.__file_path: str = file_path
        self.__data: list[dict[str, any]] = (
            self.__load_data(file_path) if not overwrite else []
        )
        self.__current_chunk: list[dict[str, any]] = None
        self.__add_id: bool = add_id

    def __load_data(self, file_path: str) -> list[dict[str, any]]:
        if not os.path.exists(os.path.abspath(file_path)):
            return []

        mode = "rb" if file_path.endswith(".jsonb") else "r"
        with open(file_path, mode) as f:
            if file_path.endswith(".json"):
                return json.load(f)
            elif file_path.endswith(".jsonb"):
                return pickle.load(f)
            else:
                raise ValueError("Invalid file extension!")

    def select(self, keys: Optional[list[str]] = None) -> "JsonDb":
        if keys is None or self.__current_chunk is None:
            self.__current_chunk = self.__data
            if keys is None:
                return self

        temp_chunk = []
        for item in self.__current_chunk:
            temp_val: dict[str, any] = {}
            for key in keys:
                if key in item:
                    temp_val[key] = item[key]

            temp_chunk.append(temp_val)

        self.__current_chunk = temp_chunk

        return self

    def where(
        self, condition: str, key: Optional[str] = "", value: Optional[any] = None
    ) -> "JsonDb":
        self.__current_chunk = []

        for item in self.__data:
            key_val = item[key] if key in item else ""
            value_val = value if value is not None else ""
            if perform_op(key_val, value_val, CONDITION_MAP[condition]):
                self.__current_chunk.append(item)

        return self

    def insert(self, data: dict[str, any]) -> None:
        if self.__add_id:
            data["__id"] = uuid.uuid4().hex
        self.__data.append(data)

    def update(self, key: str, value: any) -> "JsonDb":
        for item in self.__current_chunk:
            item[key] = value

        return self

    def delete(self) -> "JsonDb":
        for item in self.__current_chunk:
            self.__data.remove(item)

        return self

    def truncate(self) -> "JsonDb":
        self.__data = []
        return self

    def insert_batch(self, data: list[dict[str, any]]) -> None:
        for item in data:
            self.insert(item)

    def fetch_value(self) -> list[dict[str, any]]:
        return self.__current_chunk

    def commit(
        self, only_value: Optional[bool] = False, as_binary: Optional[bool] = False
    ) -> None:
        data = self.__data if not only_value else self.__current_chunk
        mode = "wb" if as_binary else "w"
        file_path = self.__file_path + "b" if as_binary else self.__file_path
        with open(file_path, mode) as f:
            if as_binary:
                pickle.dump(data, f)
            else:
                json.dump(data, f, indent=4)
