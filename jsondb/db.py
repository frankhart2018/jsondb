import json
import os


def perform_op(left_operand: any, right_operand: any, operator: str) -> bool:
    assert type(left_operand) == type(right_operand), "Type mismatch!"
    output = []

    left_operand = f'"{left_operand}"' if type(left_operand) == str else left_operand
    right_operand = (
        f'"{right_operand}"' if type(right_operand) == str else right_operand
    )
    exec(f"output.append({left_operand} {operator} {right_operand})")
    return output[0]


CONDITION_MAP = {
    "eq": "==",
    "neq": "!=",
    "lt": "<",
    "gt": ">",
    "lte": "<=",
    "gte": ">=",
}


class JsonDb:
    def __init__(self, file_path: str) -> None:
        dir_name = os.path.dirname(file_path)
        if not os.path.exists(os.path.abspath(dir_name)):
            raise ValueError(f"Directory {dir_name} does not exist!")

        self.__file_path: str = file_path
        self.__data: list[dict[str, any]] = []
        self.__current_chunk: list[dict[str, any]] = None

    def select(self, keys: list[str]) -> "JsonDb":
        self.__current_chunk = []
        for item in self.__data:
            temp_val: dict[str, any] = {}
            for key in keys:
                if key in item:
                    temp_val[key] = item[key]

            self.__current_chunk.append(temp_val)

        return self

    def where(self, condition: str, key: str, value: str) -> "JsonDb":
        self.__current_chunk = []

        for item in self.__data:
            if perform_op(item[key], value, CONDITION_MAP[condition]):
                self.__current_chunk.append(item)

        return self

    def insert(self, data: dict[str, any]) -> None:
        self.__data.append(data)

    def fetch_value(self) -> list[dict[str, any]]:
        return self.__current_chunk

    def commit(self, only_value=False) -> None:
        data = self.__data if not only_value else self.__current_chunk
        with open(self.__file_path, "w") as f:
            json.dump(data, f, indent=4)
