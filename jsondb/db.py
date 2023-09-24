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
        read_mode: Optional[bool] = False,
        is_binary: Optional[bool] = False,
    ) -> None:
        dir_name = os.path.dirname(file_path)
        if not os.path.exists(os.path.abspath(dir_name)):
            raise ValueError(f"Directory {dir_name} does not exist!")

        file_ext = file_path.split(".")[-1]
        if is_binary and file_ext != "jsonb":
            raise ValueError(f"Binary dumping requires a file with extension '.jsonb'")
        elif not is_binary and file_ext == "jsonb":
            raise ValueError(
                f"A '.jsonb' file needs to be loaded in binary mode, set is_binary to True"
            )

        self.__file_path: str = file_path
        self.__binary_mode = "" if not is_binary else "b"
        self.__data: list[dict[str, any]] = (
            self.__load_data(file_path) if not overwrite else []
        )
        self.__current_chunk: list[dict[str, any]] = None
        self.__add_id = add_id
        self.__read_mode = read_mode

    def __load_data(self, file_path: str) -> list[dict[str, any]]:
        if not os.path.exists(os.path.abspath(file_path)):
            return []

        with open(file_path, f"r{self.__binary_mode}") as f:
            return pickle.load(f) if self.__binary_mode else json.load(f)

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
        if self.__read_mode:
            raise ValueError("Cannot insert data in read mode!")

        if self.__add_id:
            data["__id"] = uuid.uuid4().hex
        self.__data.append(data)

    def update(self, key: str, value: any) -> "JsonDb":
        if self.__read_mode:
            raise ValueError("Cannot update data in read mode!")

        for item in self.__current_chunk:
            item[key] = value

        return self

    def delete(self) -> "JsonDb":
        if self.__read_mode:
            raise ValueError("Cannot delete data in read mode!")

        for item in self.__current_chunk:
            self.__data.remove(item)

        return self

    def truncate(self) -> "JsonDb":
        if self.__read_mode:
            raise ValueError("Cannot truncate data in read mode!")

        self.__data = []
        return self

    def insert_batch(self, data: list[dict[str, any]]) -> None:
        if self.__read_mode:
            raise ValueError("Cannot insert data in read mode!")

        for item in data:
            self.insert(item)

    def fetch_value(self) -> list[dict[str, any]]:
        return self.__current_chunk

    def commit(self, only_value: Optional[bool] = False) -> None:
        if self.__read_mode:
            raise ValueError("Cannot commit data in read mode!")

        data = self.__data if not only_value else self.__current_chunk
        with open(self.__file_path, f"w{self.__binary_mode}") as f:
            if self.__binary_mode:
                pickle.dump(data, f)
            else:
                json.dump(data, f, indent=4)
