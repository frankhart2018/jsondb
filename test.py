from jsondb import JsonDb


def main():
    db_obj = JsonDb("test.json")
    db_obj.insert({"name": "John", "age": 20})
    db_obj.insert({"name": "Jane", "age": 21})
    db_obj.insert({"name": "Jonathan", "age": 20})

    db_obj = (
        db_obj.select(["name", "age"])
        .where("eq", "age", 20)
        .where("eq", "name", "John")
    )
    val = db_obj.fetch_value()
    print(val)
    db_obj.commit(only_value=True)


if __name__ == "__main__":
    main()
